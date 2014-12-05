#!/usr/bin/python
import os
import string
import sys
import readline
from thrift.transport.TTransport import TTransportException
import time

sys.path.append('gen-py')

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from storm import Nimbus
from storm.ttypes import *
from storm.constants import *

topology_info = {}
print_buf = []


def print_to_buf(print_str):
    print_buf.append(print_str)


def clear_print_buf():
    del print_buf[:]


def print_buf_flush():
    print string.join(print_buf, '\n')
    print
    clear_print_buf()


class Context:
    def __init__(self, host):
        self.host = host
        if len(self.host.split(':')) == 2:
            self.port = int(self.host.split(':')[1])
        else:
            self.port = 6627
        self.client = None
        self.cwd = '/'
        self.match = None
        self.options = {
            'cd': CdOption,
            'ls': LsOption,
            'refresh': RefreshOption,
            'top': TopOption,
            'deactivate': DeactivateOption,
            'activate': ActivateOption,
            'kill': KillOption,
            'rebalance': RebalanceOption,
            'exit': ExitOption
        }

    def connect(self):
        sock = TSocket.TSocket(self.host, self.port)
        transport = TTransport.TFramedTransport(sock)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = Nimbus.Client(protocol)
        transport.open()
        init_topology_info(self.client)

    def get_option(self, token):
        return self.options.get(token)

    def get_option_list(self):
        return self.options.keys()

    def complete(self, text, state):
        buf = readline.get_line_buffer()
        tokens = buf.strip().split()
        token_len = len(tokens)
        if token_len == 0:
            self.match = self.get_option_list()
            return self.match[state]
        if buf[len(buf) - 1] == ' ':
            tokens.append('')
            token_len += 1
        elif token_len == 1:
            options = self.get_option_list()
            if state == 0:
                self.match = [option for option in options if option.startswith(text)]
                if len(self.match) <= state:
                    return None
                if len(self.match) == 1:
                    return self.match[state] + ' '
                return self.match[state]
            if len(self.match) <= state:
                return None
            return self.match[state]
        option = self.get_option(tokens[0])
        if option is None:
            return None
        if state == 0:
            self.match, next_char, args = option.complete(self.client, self.cwd, tokens[1:])
            if len(self.match) == 1:
                return self.match[state] + next_char
        if state >= len(self.match):
            return None
        return self.match[state]

    def parse(self, tokens):
        if not tokens is None:
            if len(tokens) == 0:
                return
            option = self.get_option(tokens[0])
            if not option is None:
                match, next_char, args = option.complete(self.client, self.cwd, tokens[1:])
                self.cwd = option.execute(self.client, self.cwd, args)
            else:
                unknown_option(tokens[0])


def unknown_option(option):
    print 'Unknown option: %s' % option


def check_is_root(path):
    return os.path.dirname(path) == path


def join_path(paths):
    return '/' + string.join(paths, '/')


def get_complete_list_test(client, tokens):
    a = {
        'oid_count': {
            'kafka': {},
            'redis': {}
        },
        'user_relation': {
            'count': {}
        },
        'user_follow': {
            'follow': {}
        }
    }
    t = a
    for token in tokens:
        if token in t:
            t = t.get(token)
        else:
            return []
    return t.keys()


def init_topology_info(client):
    topology_info.clear()
    for topology in client.getClusterInfo().topologies:
        topology_info[topology.name] = {}
        for component in client.getTopologyInfo(topology.id).executors:
            topology_info[topology.name][component.component_id] = {}


def get_complete_list(client, tokens):
    info = topology_info
    for token in tokens:
        if token in info:
            info = info.get(token)
        else:
            return []
    return info.keys()


def complete_path(client, real_path, top=True, is_root=False):
    if is_root or check_is_root(real_path):
        # List all topology
        return get_complete_list(client, []), []
    dir_name, base_name = os.path.split(real_path)
    path, tokens = complete_path(client, dir_name, top=False)
    if base_name == '..':
        if top:
            return ['..'], tokens[0:-1]
        else:
            dir_name, base_name = os.path.split(dir_name)
            if len(tokens) <= 1:
                path, tokens = complete_path(client, dir_name, top=False, is_root=True)
            else:
                path, tokens = complete_path(client, dir_name, top=False)
            return path, tokens
    if top:
        if base_name == '':
            return [i for i in get_complete_list(client, tokens) if i.startswith(base_name)], tokens
        else:
            if base_name in get_complete_list(client, tokens):
                return [i for i in get_complete_list(client, tokens) if i.startswith(base_name)], tokens + [base_name]
            return [i for i in get_complete_list(client, tokens) if i.startswith(base_name)], tokens + [None]
    if base_name in get_complete_list(client, tokens):
        return [base_name], tokens + [base_name]
    return [], tokens + [None]


class OptionBase:
    @staticmethod
    def execute(client, args):
        pass

    @staticmethod
    def complete(client, cwd, args):
        if len(args) == 0:
            real_path = os.path.join(cwd, '')
        else:
            real_path = os.path.join(cwd, args[0])
        path, tokens = complete_path(client, real_path)
        return path, '/', tokens


class CdOption(OptionBase):
    @staticmethod
    def execute(client, cwd, args):
        if len(args) == 0:
            return '/'
        if args[-1] is None:
            print 'No such path!'
            return cwd
        return join_path(args)


class LsOption(OptionBase):
    @staticmethod
    def execute(client, cwd, args):
        for i in sorted(get_complete_list(client, args)):
            print i
        return cwd


class RefreshOption(OptionBase):
    @staticmethod
    def execute(client, cwd, args):
        init_topology_info(client)
        return cwd

    @staticmethod
    def complete(client, cwd, args):
        return [], '/', []


class TopOption(OptionBase):
    @staticmethod
    def execute(client, cwd, args):
        if len(args) == 0:
            cluster_top(client)
            return cwd
        if args[-1] is None:
            print 'No such Topology.'
            return cwd
        if len(args) == 1:
            try:
                topology_top(client, args[0])
            except TTransportException:
                print 'Error occurs.'
            return cwd
        return cwd


def confirm_operation():
    confirm = raw_input('Confirm your operation(y/n): ')
    if confirm == 'y':
        return True
    print 'Canceled'
    return False


def deactivate_topology(client, topology):
    if topology is None:
        print 'No such topology.'
        return
    if not confirm_operation():
        return
    client.deactivate(topology)
    print 'Deactivate %s success.' % topology


class DeactivateOption(OptionBase):
    @staticmethod
    def execute(client, cwd, args):
        if len(args) == 0:
            return cwd
        deactivate_topology(client, args[0])
        return cwd


def activate_topology(client, topology):
    if topology is None:
        print 'No such topology.'
        return
    client.activate(topology)
    print 'Activate %s success.' % topology


class ActivateOption(OptionBase):
    @staticmethod
    def execute(client, cwd, args):
        if len(args) == 0:
            return cwd
        activate_topology(client, args[0])
        return cwd


def kill_topology(client, topology):
    if topology is None:
        print 'No such topology.'
        return
    if not confirm_operation():
        return
    wait_secs = raw_input('Specify wait seconds before kill %s: ' % topology)
    try:
        thrift_kill_options = KillOptions(wait_secs=int(wait_secs))
        client.killTopologyWithOpts(topology, thrift_kill_options)
        print 'Kill %s success.' % topology
    except ValueError:
        print 'Not a integer value.'


class KillOption(OptionBase):
    @staticmethod
    def execute(client, cwd, args):
        if len(args) == 0:
            return cwd
        kill_topology(client, args[0])
        return cwd


def rebalance_topology(client, topology):
    if topology is None:
        print 'No such topology.'
        return
    wait_secs = raw_input('Specify wait seconds before rebalance %s: ' % topology)
    try:
        thrift_rebalance_options = RebalanceOptions(wait_secs=int(wait_secs))
        client.rebalance(topology, thrift_rebalance_options)
        print 'Rebalance %s success.' % topology
    except ValueError:
        print 'Not a integer value.'


class RebalanceOption(OptionBase):
    @staticmethod
    def execute(client, cwd, args):
        if len(args) == 0:
            return cwd
        rebalance_topology(client, args[0])
        return cwd


class ExitOption(OptionBase):
    @staticmethod
    def complete(client, cwd, args):
        return [], '/', []

    @staticmethod
    def execute(client, cwd, args):
        exit(0)


def fill_space(item, length):
    for i in xrange(length - len(item)):
        item += ' '
    return item


def print_info(info, max_length):
    print_str = ''
    sorted_max_length = sorted(max_length.items(), cmp=lambda x, y: cmp(x[1][1], y[1][1]))
    for i in sorted_max_length:
        print_str += fill_space(i[0], i[1][0])
    print_to_buf(print_str)
    for i in info:
        print_str = ''
        for j in sorted_max_length:
            if isinstance(info[i][j[0]], int):
                print_str += fill_space('%d' % info[i][j[0]], j[1][0])
            elif isinstance(info[i][j[0]], str) or isinstance(info[i][j[0]], unicode):
                print_str += fill_space(info[i][j[0]], j[1][0])
            elif isinstance(info[i][j[0]], float):
                print_str += fill_space('%.3f' % info[i][j[0]], j[1][0])
        print_to_buf(print_str)


def print_topology_summary(client, specific=None):
    print_to_buf('Topology Summary')
    topologies = {}
    max_length = {
        'Name': [6, 0],
        'Status': [7, 1],
        'Uptime': [8, 2],
        'Num workers': [13, 3],
        'Num executors': [15, 4],
        'Num tasks': [10, 5]
    }
    for i in client.getClusterInfo().topologies:
        if not specific is None and i.name != specific:
            continue
        topologies[i.name] = {
            'Name': i.name,
            'Status': i.status,
            'Num workers': '%d' % i.num_workers,
            'Num executors': '%d' % i.num_executors,
            'Uptime': '%dd %dh %dm %ds' % (
                i.uptime_secs / 60 / 60 / 24, i.uptime_secs / 60 / 60 % 24, i.uptime_secs / 60 % 60,
                i.uptime_secs % 60),
            'Num tasks': '%d' % i.num_tasks
        }
        for j in topologies[i.name]:
            if len(topologies[i.name][j]) + 2 > max_length[j][0]:
                max_length[j][0] = len(topologies[i.name][j]) + 2
    print_info(topologies, max_length)


def print_supervisor_summary(client):
    print_to_buf('Supervisor summary')
    supervisors = {}
    max_length = {
        'Host': [6, 0],
        'Uptime': [8, 1],
        'Slots': [7, 2],
        'Used Slots': [12, 3]
    }
    for i in client.getClusterInfo().supervisors:
        supervisors[i.host] = {
            'Host': i.host,
            'Uptime': '%dd %dh %dm %ds' % (
                i.uptime_secs / 60 / 60 / 24, i.uptime_secs / 60 / 60 % 24, i.uptime_secs / 60 % 60,
                i.uptime_secs % 60),
            'Slots': '%d' % i.num_workers,
            'Used Slots': '%d' % i.num_used_workers
        }
        for j in supervisors[i.host]:
            if len(supervisors[i.host][j]) + 2 > max_length[j][0]:
                max_length[j][0] = len(supervisors[i.host][j]) + 2
    print_info(supervisors, max_length)


def get_topology_id(client, topology):
    for i in client.getClusterInfo().topologies:
        if i.name == topology:
            return i.id
    return None


def get_executors_info(client, executors):
    executors_info = {
        'Spouts': {},
        'Bolts': {}
    }
    max_length = {
        'Spouts': {
            'Id': [4, 0],
            'Executors': [11, 1],
            'Tasks': [7, 2],
            'Emitted(10min)': [16, 3],
            'Emitted(All)': [14, 4]
        },
        'Bolts': {
            'Id': [4, 0],
            'Executors': [11, 1],
            'Tasks': [7, 2],
            'Emitted(10min)': [16, 3],
            'Emitted(All)': [14, 4],
            'Capacity(Max)': [15, 5],
            'Capacity(Avg)': [15, 6],
            'Execute latency(10min)': [24, 7],
            'Executed(1s)': [14, 8],
            'Executed(10min)': [17, 9],
            'Executed(All)': [14, 10],
        }
    }
    for executor in executors:
        component_info = {}
        component_name = executor.component_id
        component_info['Id'] = component_name
        component_info['Executors'] = executor.executor_info.task_end - executor.executor_info.task_start + 1
        component_info['Tasks'] = 1
        if executor.stats.specific is None:
            continue
        if not executor.stats.specific.spout is None:
            if 'default' in executor.stats.emitted['600']:
                component_info['Emitted(10min)'] = executor.stats.emitted['600']['default']
            else:
                component_info['Emitted(10min)'] = 0
            if 'default' in executor.stats.emitted[':all-time']:
                component_info['Emitted(All)'] = executor.stats.emitted[':all-time']['default']
            else:
                component_info['Emitted(All)'] = 0
            if component_name in executors_info['Spouts']:
                executors_info['Spouts'][component_name]['Executors'] += component_info['Executors']
                executors_info['Spouts'][component_name]['Tasks'] += 1
                executors_info['Spouts'][component_name]['Emitted(10min)'] += component_info['Emitted(10min)']
                executors_info['Spouts'][component_name]['Emitted(All)'] += component_info['Emitted(All)']
            else:
                executors_info['Spouts'][component_name] = component_info
            if max_length['Spouts']['Id'][0] < len(component_name) + 2:
                max_length['Spouts']['Id'][0] = len(component_name) + 2
            if max_length['Spouts']['Executors'][0] < len(
                            '%d' % executors_info['Spouts'][component_name]['Executors']) + 2:
                max_length['Spouts']['Executors'][0] = len(
                    '%d' % executors_info['Spouts'][component_name]['Executors']) + 2
            if max_length['Spouts']['Tasks'][0] < len('%d' % executors_info['Spouts'][component_name]['Tasks']) + 2:
                max_length['Spouts']['Tasks'][0] = len('%d' % executors_info['Spouts'][component_name]['Tasks']) + 2
            if max_length['Spouts']['Emitted(10min)'][0] < len(
                            '%d' % executors_info['Spouts'][component_name]['Emitted(10min)']) + 2:
                max_length['Spouts']['Emitted(10min)'][0] = len(
                    '%d' % executors_info['Spouts'][component_name]['Emitted(10min)']) + 2
            if max_length['Spouts']['Emitted(All)'][0] < len(
                            '%d' % executors_info['Spouts'][component_name]['Emitted(All)']) + 2:
                max_length['Spouts']['Emitted(All)'][0] = len(
                    '%d' % executors_info['Spouts'][component_name]['Emitted(All)']) + 2
        else:
            if 'default' in executor.stats.emitted['600']:
                component_info['Emitted(10min)'] = executor.stats.emitted['600']['default']
            else:
                component_info['Emitted(10min)'] = 0
            if 'default' in executor.stats.emitted[':all-time']:
                component_info['Emitted(All)'] = executor.stats.emitted[':all-time']['default']
            else:
                component_info['Emitted(All)'] = 0
            component_info['Executed(10min)'] = 0
            component_info['Executed(1s)'] = 0
            component_info['Execute latency(10min)'] = 0
            component_info['Capacity(Max)'] = 0
            for i in executor.stats.specific.bolt.executed['600']:
                component_info['Capacity(Max)'] += executor.stats.specific.bolt.executed['600'][i] * \
                                                   executor.stats.specific.bolt.execute_ms_avg['600'][i] / 600000
                component_info['Execute latency(10min)'] = (component_info['Executed(10min)'] * component_info[
                    'Execute latency(10min)'] + executor.stats.specific.bolt.executed['600'][i] *
                                                            executor.stats.specific.bolt.execute_ms_avg['600'][i]) / (
                                                               component_info['Executed(10min)'] +
                                                               executor.stats.specific.bolt.executed['600'][i])
                component_info['Executed(10min)'] += executor.stats.specific.bolt.executed['600'][i]
                component_info['Executed(1s)'] = component_info['Executed(10min)'] / 600
            component_info['Capacity(Avg)'] = component_info['Capacity(Max)']
            component_info['Executed(All)'] = 0
            for i in executor.stats.specific.bolt.executed[':all-time']:
                component_info['Executed(All)'] += executor.stats.specific.bolt.executed[':all-time'][i]
            if component_name in executors_info['Bolts']:
                executors_info['Bolts'][component_name]['Capacity(Avg)'] = (executors_info['Bolts'][component_name][
                                                                                'Capacity(Avg)'] *
                                                                            executors_info['Bolts'][component_name][
                                                                                'Executors'] + component_info[
                                                                                'Capacity(Avg)']) / (
                                                                               executors_info['Bolts'][component_name][
                                                                                   'Executors'] + 1)
                executors_info['Bolts'][component_name]['Executors'] += component_info['Executors']
                executors_info['Bolts'][component_name]['Tasks'] += 1
                executors_info['Bolts'][component_name]['Emitted(10min)'] += component_info['Emitted(10min)']
                executors_info['Bolts'][component_name]['Emitted(All)'] += component_info['Emitted(All)']
                if executors_info['Bolts'][component_name]['Executed(10min)'] + component_info['Executed(10min)'] == 0:
                    executors_info['Bolts'][component_name]['Execute latency(10min)'] = 0.0
                else:
                    executors_info['Bolts'][component_name]['Execute latency(10min)'] = \
                        (executors_info['Bolts'][component_name]['Execute latency(10min)'] *
                         executors_info['Bolts'][component_name]['Executed(10min)'] + \
                         component_info['Executed(10min)'] * component_info['Execute latency(10min)']) / (
                            executors_info['Bolts'][component_name]['Executed(10min)'] + component_info['Executed(10min)'])
                executors_info['Bolts'][component_name]['Executed(10min)'] += component_info['Executed(10min)']
                executors_info['Bolts'][component_name]['Executed(1s)'] = executors_info['Bolts'][component_name][
                                                                              'Executed(10min)'] / 600
                executors_info['Bolts'][component_name]['Executed(All)'] += component_info['Executed(All)']
                if executors_info['Bolts'][component_name]['Capacity(Max)'] < component_info['Capacity(Max)']:
                    executors_info['Bolts'][component_name]['Capacity(Max)'] = component_info['Capacity(Max)']
            else:
                executors_info['Bolts'][component_name] = component_info
            if max_length['Bolts']['Id'][0] < len(component_name) + 2:
                max_length['Bolts']['Id'][0] = len(component_name) + 2
            if max_length['Bolts']['Executors'][0] < len(
                            '%d' % executors_info['Bolts'][component_name]['Executors']) + 2:
                max_length['Bolts']['Executors'][0] = len(
                    '%d' % executors_info['Bolts'][component_name]['Executors']) + 2
            if max_length['Bolts']['Tasks'][0] < len('%d' % executors_info['Bolts'][component_name]['Tasks']) + 2:
                max_length['Bolts']['Tasks'][0] = len('%d' % executors_info['Bolts'][component_name]['Tasks']) + 2
            if max_length['Bolts']['Emitted(10min)'][0] < len(
                            '%d' % executors_info['Bolts'][component_name]['Emitted(10min)']) + 2:
                max_length['Bolts']['Emitted(10min)'][0] = len(
                    '%d' % executors_info['Bolts'][component_name]['Emitted(10min)']) + 2
            if max_length['Bolts']['Emitted(All)'][0] < len(
                            '%d' % executors_info['Bolts'][component_name]['Emitted(All)']) + 2:
                max_length['Bolts']['Emitted(All)'][0] = len(
                    '%d' % executors_info['Bolts'][component_name]['Emitted(All)']) + 2
            if max_length['Bolts']['Executed(1s)'][0] < len(
                            '%d' % executors_info['Bolts'][component_name]['Executed(1s)']) + 2:
                max_length['Bolts']['Executed(1s)'][0] = len(
                    '%d' % executors_info['Bolts'][component_name]['Executed(1s)']) + 2
            if max_length['Bolts']['Executed(10min)'][0] < len(
                            '%d' % executors_info['Bolts'][component_name]['Executed(10min)']) + 2:
                max_length['Bolts']['Executed(10min)'][0] = len(
                    '%d' % executors_info['Bolts'][component_name]['Executed(10min)']) + 2
            if max_length['Bolts']['Executed(All)'][0] < len(
                            '%d' % executors_info['Bolts'][component_name]['Executed(All)']) + 2:
                max_length['Bolts']['Executed(All)'][0] = len(
                    '%d' % executors_info['Bolts'][component_name]['Executed(All)']) + 2
            if max_length['Bolts']['Execute latency(10min)'][0] < len(
                            '%.3f' % executors_info['Bolts'][component_name]['Execute latency(10min)']) + 2:
                max_length['Bolts']['Execute latency(10min)'][0] = len(
                    '%.3f' % executors_info['Bolts'][component_name]['Execute latency(10min)']) + 2
            if max_length['Bolts']['Capacity(Max)'][0] < len(
                            '%3.f' % executors_info['Bolts'][component_name]['Capacity(Max)']) + 2:
                max_length['Bolts']['Capacity(Max)'][0] = len(
                    '%3.f' % executors_info['Bolts'][component_name]['Capacity(Max)']) + 2
            if max_length['Bolts']['Capacity(Avg)'][0] < len(
                            '%3.f' % executors_info['Bolts'][component_name]['Capacity(Avg)']) + 2:
                max_length['Bolts']['Capacity(Avg)'][0] = len(
                    '%3.f' % executors_info['Bolts'][component_name]['Capacity(Avg)']) + 2

    return executors_info, max_length


def print_spouts_stats(spouts_stats, max_length):
    print_to_buf('Spouts')
    print_info(spouts_stats, max_length)


def print_bolts_stats(bolts_stats, max_length):
    print_to_buf('Bolts')
    print_info(bolts_stats, max_length)


def print_topology_stats(client, topology):
    topology_id = get_topology_id(client, topology)
    topology_info = client.getTopologyInfo(topology_id)
    executors = topology_info.executors
    executors_info, max_length = get_executors_info(client, executors)
    print_to_buf('')
    print_spouts_stats(executors_info['Spouts'], max_length['Spouts'])
    print_to_buf('')
    print_bolts_stats(executors_info['Bolts'], max_length['Bolts'])


def cluster_top(client):
    while True:
        try:
            print_topology_summary(client)
            print_to_buf('')
            print_supervisor_summary(client)
            print_to_buf('')
            os.system('clear')
            print_buf_flush()
            time.sleep(5)
        except KeyboardInterrupt:
            break


def topology_top(client, topology):
    while True:
        try:
            print_topology_summary(client, specific=topology)
            print_to_buf('')
            print_topology_stats(client, topology)
            print_to_buf('')
            os.system('clear')
            print_buf_flush()
            time.sleep(5)
        except KeyboardInterrupt:
            break


def prompt(host, cwd):
    return '%s:%s>' % (host, cwd)


def print_usage():
    print "Usage: ./storm-cli Host[:Port] [cmd [arg [arg ...]]]"


if __name__ == '__main__':
    context = None
    if len(sys.argv) < 2:
        print_usage()
        exit(0)
    elif len(sys.argv) == 2:
        try:
            context = Context(sys.argv[1])
        except ValueError:
            print_usage()
            exit(0)
    else:
        try:
            context = Context(sys.argv[1])
            context.connect()
            context.parse(sys.argv[2:])
            exit(0)
        except ValueError:
            print "Usage: ./storm-cli Host[:Port] [cmd [arg [arg ...]]]"
            exit(0)
        except TTransportException:
            print "Could not connect to nimbus %s:%d" % (context.host, context.port)
            exit(0)
    try:
        context.connect()
    except TTransportException:
        print "Could not connect to nimbus %s:%d" % (context.host, context.port)
        exit(0)
    readline.set_completer(context.complete)
    readline.parse_and_bind("tab: complete")
    while True:
        try:
            line = raw_input(prompt(context.host, context.cwd))
        except EOFError:
            print
            exit(0)
        line = line.strip()
        tokens = line.split()
        context.parse(tokens)
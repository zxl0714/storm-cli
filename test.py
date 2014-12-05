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

sock = TSocket.TSocket('10.77.136.48', 6627)
transport = TTransport.TFramedTransport(sock)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Nimbus.Client(protocol)
transport.open()
executors = client.getTopologyInfo('oid_count_test-21-1417750156').executors
for executor in executors:
    component_name = executor.component_id
    if component_name != 'CopyBolt':
        continue
    print executor.stats.emitted
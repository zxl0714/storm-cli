# storm-cli

storm-cli是storm的一个命令行工具,可以通过命令行查看storm中各个supervisor,topology,spout以及bolt的运行状态.

## 安装依赖

storm-cli依赖于thrift-storm,它是thrift-0.7.0的storm定制版,不使用此thrift版本生成的代码是错误的.

使用其它版本的thrift虽然不能生成正确的代码,但是可以使用已生成的代码通信,所以可能不影响使用,这点还需验证.

解包

    $ unzip thrift-storm.zip
    $ cd thrift-storm

打补丁

    $ patch -p0 < ../thrift-1382.patch 

编译安装

    $ ./bootstrap.sh 
    $ ./configure --without-ruby 
    $ make 
    $ make install 
    $ cd lib/py 
    $ python setup.py install

## 快速入门

本命令行工具带自动补全功能,可以使用tab键快速补全命令,拓扑名,component名等.

### 运行

    $ ./storm-cli Host[:Port] [cmd [arg [arg ...]]]

### 查看列表: ls

    10.77.96.30:/>ls
    10.77.96.30:/>ls news_count/
    10.77.96.30:/news_count>ls

### 查看状态信息: top

    10.77.96.30:/>top
    10.77.96.30:/>top news_count/
    10.77.96.30:/news_count>top

此命令会实时获取storm的信息.

    Topology Summary
    Name       Status  Uptime         Num workers  Num executors  Num tasks 
    oid_count  ACTIVE  1d 5h 19m 54s  24           1978           1978      
    

    Spouts
    Id          Executors  Tasks  Emitted(10min)  Emitted(All)  
    KafkaSpout  13         13     25471540        4161909860    
    
    Bolts
    Id                  Executors  Tasks  Emitted(10min)  Emitted(All)  Capacity(Max)  Capacity(Avg)  Execute latency(10min)  Executed(1s)  Executed(10min)  Executed(All) 
    redisDumper         10         10     0               0             0.136          0.128          0.134                   9573          5743800          969995580     
    ProgramFilterBolt   150        150    21352480        2936184400    0.108          0.091          0.321                   42463         25477880         4161905620    
    OidsCountBolt       10         10     69593260        10980638420   0.136          0.115          0.090                   12699         7619660          1200681500    
    TrendDumper         1500       1500   0               0             0.854          0.370          49.054                  11311         6786960          1066229800    
    TopicFilterBolt     150        150    7611780         1200571560    0.126          0.111          0.390                   42466         25480180         4161907820    
    __acker             1          1      0               0             0.000          0.000          0.000                   0             20               3480          
    CopyBolt            20         20     25470620        4161896760    0.040          0.034          0.016                   42451         25470660         4161896920    
    OidsSumBolt         10         10     5739420         969993260     0.068          0.058          0.005                   115835        69501320         10980571080   
    UqualityFilterBolt  4          4      6792700         1066235840    0.106          0.095          0.030                   12696         7617940          1200577100    
    HdfsDumper          10         10     0               0             0.323          0.139          0.039                   35628         21377200         2936228360    
    RedisHllDumper      100        100    0               0             0.220          0.202          1.595                   12686         7612000          1200571840 

如上显示的是一个topology的top输出,定时刷新,使用ctrl c退出.

### 进入命令: cd

    10.77.96.30:/>cd news_count/
    10.77.96.30:/>cd news_count/commentFirehoseSpout/
    10.77.96.30:/news_count/commentFirehoseSpout>cd ../../mblog_count/

### 刷新命令: refresh

    10.77.96.30:/>refresh

使用这个命令可以刷新storm的信息,例如submit或者kill一个topology后使用次命令才在自动补全以及ls的时候正常显示.

### Deactivate命令: deactivate

    10.77.96.30:/>deactivate test_topology/
    Confirm your operation(y/n): y
    Deactivate test_topology success.


### Activate命令: activate

    10.77.96.30:/>activate test_topology/
    Activate test_topology success.

### Rebalance命令: rebalance

    10.77.96.30:/>rebalance test_topology/
    Specify wait seconds before rebalance test_topology: 30
    Rebalance test_topology success.

### Kill命令: kill

    10.77.96.30:/>kill test_topology/
    Confirm your operation(y/n): y
    Specify wait seconds before kill test_topology: 30
    Kill test_topology success.

### 退出命令: exit

    10.77.96.30:/>exit

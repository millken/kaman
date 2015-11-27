#kaman


Implementation
==============
Overview
--------

```
Input -> Router -> Output
```
Data flow
---------

```
                        -------<-------- 
                        |               |
                        V               | generate pool
       InputRunner.inputRecycleChan     | recycling
            |           |               |        \  
            |            ------->-------          \ 
            |               ^           ^          \
    InputRunner.inChan      |           |           \
            |               |           |            \
            |               |           |             \
    consume |               |           |              \
            V               |           |               \
          Input(Router.inChan) ---->  Router  ----> (Router.outChan)Output.inChan

```
Simple
==============

```
[testinput]
type = "TailsInput"
log_directory = "/nginx/logs/"
file_match = '(?P<DomainName>[^ ]*)/(?P<name>[^_/]+)\.log'
journal_directory = "/tmp/aaa/"
# file_match = '(.*?)\.log'
tag = "t3"
# while :; do     echo  $RANDOM >> error.log ; sleep 0.5 ; done
# while :; do       date >> nginx.error.log ; sleep 2 ; done

[testoutput]
tag = "t3"
type = "StdoutOutput"
decoder = "regexcoder1"
encoder = "regexcoder2"

[test2output]
tag = "t31"
type = "UdpOutput"
address = "192.168.3.50:60100"

[testoutputdecoder]
decoder = "regexcoder1"
match_regex = "(?P<week>[^ ]*) (?P<month>[^ ]*) (?P<day>[^ ]*) (?P<hour>\\d{2}):(?P<minute>\\d{2}):(?P<second>\\d{2}) CST (?P<year>\\d{2})"
type = "RegexDecoder"

[testoutputencoder]
encoder = "regexcoder2"
type = "JsonEncoder"

[test3output]
tag = "t33"
type = "KafkaOutput"
addrs = ["2b29c96f2099:60092","2b29c96f2099:60093","2b29c96f2099:60094"]
topic = "dddd"
partition = 1
# partitions = 3
distributer = "RoundRobin"
```

# SICS-to-Kafka

Forwards published SICS messages to Kafka. Parameter updates are sent to topic 'sics-stream' by default as 'f142' log message events. Parameter sets values and units are dumped to topic 'sics-units' periodically as 'json' messages. A single json message contains all the parameters and units at a single point in time. 

## usage
```
   -h,--help           Print this help message and exit
   --broker TEXT       Kafka broker as url:port
   --sics URL          SICS URL
   --port              Port # for the ZMQ publisher
   --topic             Kafka topic to store the messages
```

## Design decisions
SICS maintains a ZeroMQ publish/subscribe port (5566) which provides parameters, states and status information. 
The parameter value data does not include the 'units' - this information has to be requested for the parameter
of interest through a second port (5555) and is returned as an XML message. This service is also used to recover
the parameters that should be saved to the HDF file by 'getgumtreexml /' and to be replaced with 'getkafkaxml /'.

The main components within the code are:

#### client.py 
The module subscribes to the SICS stream and monitors the message flow. Value messages are forwarded to kafka and the 
unit manager while state messages are forwarded to the state processor.

#### state.py
The module monitors the state messages to capture the start and end of a HMS scan session. On receiving a start 
message the module collects the parameters to be saved from the SICS server, checks that the units are valid.

#### cmdbuilder.py
Contains the command builder use to construct the write command message understood by the kafk-to-nexus writer.

## Test
To simplify connections through the firewall on the VM setup two ssh tunnels for ports 5566 and 5555 by:

$ ssh -L 5566:ics1-pelican-test.nbi.ansto.gov.au:5566 geishm@ics1-pelican-test.nbi.ansto.gov.au


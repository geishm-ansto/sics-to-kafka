# SICS-to-Kafka

Forwards published SICS messages to Kafka. All the messages are forwarded
as log events using:

- f142, log data flat buffer 

## usage
```
   -h,--help           Print this help message and exit
   --broker TEXT       Kafka broker as url:port
   --sics TEXT         SICS addr and port as url:port
   --topic             Kafka topic to store the messages
```
## Design decisions
SICS maintains a ZeroMQ publish/subscribe port (5566) which provides parameters, states and status information. 
The parameter value data does not include the 'units' - this information has to be requested for the parameter
of interest through a second port (5555) and is returned as an XML message. 

The units are only needed to build the command string to save events in Kafka to a Nexus file. The module maintains 
dictionary of units keyed by the parameter name. The units are invalidated when SICS restarts - so this dictionary 
needs to be rebuilt on this event and the dictionary needs to be completely updated before a Nexus file can be saved.



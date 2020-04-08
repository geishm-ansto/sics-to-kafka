# SICS-to-Kafka

Forwards published SICS messages to Kafka as defined in a json configuration file. All the messages are forwarded
as log events using:

- f142, log data flat buffer 

## usage
```
   -h,--help           Print this help message and exit
   --broker TEXT       Kafka broker as url:port
   --sics TEXT         SICS addr and port as url:port
   --topic             Kafka topic to store the messages
```




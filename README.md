# SICS-to-Kafka

Forwards published SICS messages to Kafka as defined in a json configuration file. The messages are forwarded
as log events using:

- f142, log data flat buffer 

## usage
```
   -h,--help                   Print this help message and exit
   -b,--broker TEXT            Kafka broker
   --streams-json TEXT         Json file for streams to be added
```

## Add messages to the list to be forwarded

The add PVs command consists of the following parameters which are defined as key-value pairs in JSON:

- cmd: The command name, must be `add`
- streams: The PV streams to add. This is a list of dictionaries where each dictionary represents one PV.

```json
{
  "streams": [
    {
      "channel": "MYIOC:VALUE1",
      "schema": "f142",
      "topic": "sics_params"

    },
    {
      "channel": "MYIOC:VALUE2",
      "schema": "f142",
      "topic": "some_topic"
    }
  ]
}
```



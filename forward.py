#!/usr/bin/python
"""
  This is a little program which listens to a ZeroMQ stream for SICS parameters and forwards
  the messages to specified Kafka topic.

  Each message follows a fixed json format:
  { "type": "Value", "name": "param_name", "value": "0", "seq": 572, "ts": 1585519280.073885 }
    - "type" : [Value, State, Status]
      - only recording 'Value' types for now
    - "name" maps to nexus format in most cases, such as 
          "/instrument/detector/detector_x/ignorefault"
      - use this as the source name in the kafka message
    - "value" can be int, float, string - above should be bool but is string - bool exists?
    - "seq" is an incremental number for catching missed messages
    - "ts" is seconds since epoch as a double 

  The type 
"""
import argparse
import json
import zmq
import flatbuffers
from kafka import KafkaProducer

import pyschema.LogData as LogData
from pyschema.Value import Value
from pyschema.Int import IntStart, IntAddValue, IntEnd
from pyschema.Float import FloatStart, FloatAddValue, FloatEnd
from pyschema.String import StringStart, StringAddValue, StringEnd

def timestamp_to_nsecs(ts):
    return int(ts * 1e9)

def timestamp_to_msecs(ts):
    return int(ts * 1e3)

def add_int_value(builder, value):
    IntStart(builder)
    IntAddValue(builder, value)
    position = IntEnd(builder)
    return position, Value.Int

def add_float_value(builder, value):
    FloatStart(builder)
    FloatAddValue(builder, value)
    position = FloatEnd(builder)
    return position, Value.Float

def add_string_value(builder, value):
    svalue = builder.CreateString(value)
    StringStart(builder)
    StringAddValue(builder, svalue)
    position = StringEnd(builder)
    return position, Value.String

MapValue = {
    int : add_int_value,
    float : add_float_value,
    str : add_string_value,
}

def create_f142_message(timestamp, tag, value):

    file_identifier = b"f142"
    builder = flatbuffers.Builder(1024)
    source = builder.CreateString(tag)    
    try:
        (posn, val_type) = MapValue[type(value)](builder, value)
    except (KeyError):
        print('No suitable builder for type {}'.format(type(value)))
        return None

    # Build the actual buffer
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, posn)
    LogData.LogDataAddValueType(builder, val_type)
    LogData.LogDataAddTimestamp(builder, timestamp_to_nsecs(timestamp))
    log_msg = LogData.LogDataEnd(builder)
    builder.Finish(log_msg)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return bytes(buff)

def publish_f142_message(producer, topic, timestamp, tag, value):
    """
    Publish an f142 message to a given topic.
    """
    msg = create_f142_message(timestamp, tag, value)
    if msg:
        producer.send(topic, msg, timestamp_ms=timestamp_to_msecs(timestamp))
        # Flush producer queue after each message, we don't want the messages to be batched 
        producer.flush()

def zeromq_client(args):

    try:
        producer = KafkaProducer(bootstrap_servers=[args.broker])
    except AttributeError:
        raise ValueError('Missing broker argument')

    print("Collecting updates from SICS at {}".format(args.sics))
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://' + args.sics)

    # filter messages by topic
    socket.setsockopt(zmq.SUBSCRIBE, b"")

    while True:
        message = socket.recv().decode('utf-8')
        response = json.loads(message)
        print("Message: {}".format(message))
        print("JSON: {}".format(response))
        if response["type"] == "Value":
            publish_f142_message(producer, args.topic, response['ts'], response['name'], response['value'])
            print("Value: {}: {}".format(response["name"], response['value']))
        else:
            print("Unsupported: {}".format(str(response)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Forward SICS messages to Kafka')
    parser.add_argument('--broker', help='The Kafka Broker to connect to IP:P', default='localhost:9092')
    parser.add_argument('--sics', help='SICS publish to connect ZMQ to IP:P', default='localhost:5556')
    parser.add_argument('--topic', help='Publish to Kafka topic', default='sics_stream')
    args = parser.parse_args()

    zeromq_client(args)


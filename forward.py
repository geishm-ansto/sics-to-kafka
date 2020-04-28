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
import warnings

from kafkahelp import KafkaLogger
from sicsstate import StateProcessor


def sics_client(sics, port, broker, topic, state_processor):

    kafka = KafkaLogger(broker)

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://{}:{}'.format(sics, port))

    # filter messages by topic
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    print("Collecting updates from SICS at {}".format(sics))

    while True:
        message = socket.recv().decode('utf-8')
        response = json.loads(message)
        print("{:.4f}: {}: {}: {}".format(response["ts"], response["type"], response["name"], response['value']))
        if response["type"] == "Value":
            kafka.publish_f142_message(
                topic, response['ts'], response['name'], response['value'])
        elif state_processor and response["type"] in ["State", "Status"]:
            state_processor.add_event(response)
        else:
            warnings.warn("Unsupported: {}".format(str(response)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Forward SICS messages to Kafka')
    parser.add_argument(
        '--broker', help='The Kafka Broker to connect to IP:P', default='localhost:9092')
    parser.add_argument(
        '--sics', help='SICS url for zmq publisher', default='localhost')
    parser.add_argument(
        '--port', help='SICS port for zmq publisher', default=5566)
    parser.add_argument(
        '--xport', help='SICS gumtree xml request server port', default=5555)
    parser.add_argument(
        '--topic', help='Publish to Kafka topic', default='sics_stream')
    args = parser.parse_args()

    state_processor = StateProcessor(args.sics, args.xport)
    sics_client(args.sics, args.port, args.broker, args.topic, state_processor)

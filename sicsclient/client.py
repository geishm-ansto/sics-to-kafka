#!/usr/bin/python
"""
  The program listens to a ZeroMQ stream and simply forwards the
  {State, Status} messages to the StateProcessor or the {Value}
  messages to the UnitManager objects.
"""
import argparse
import json
import zmq
import warnings
import time

from sicsclient.kafkahelp import KafkaLogger
from sicsclient.state import StateProcessor
from sicsclient.units import UnitManager


def sics_client(sics, port, state_processor, unit_manager):

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://{}:{}'.format(sics, port))

    # filter messages by type
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    print("Collecting updates from SICS at {}".format(sics))

    while True:
        try:
            message = socket.recv().decode('utf-8')
            response = json.loads(message)
            print("{:.4f}: {}: {}: {}".format(
                response["ts"], response["type"], response["name"], response['value']))
            if response["type"] == "Value":
                unit_manager.new_value_event(response)
            elif state_processor and response["type"] in ["State", "Status"]:
                state_processor.add_event(response)
            else:
                warnings.warn("Unsupported: {}".format(str(response)))
        except Exception as e:
            print(e)
            time.sleep(1)
            # need to restablish connection??
            # TODO

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
        '--topic', help='Publish to Kafka topic', default='sics-stream')
    parser.add_argument(
        '--base', help='Base command file format', default='../config/base_file.json')
    args = parser.parse_args()

    unit_manager = UnitManager(args.broker)
    state_processor = StateProcessor(
        args.sics, args.xport, args.base, unit_manager, kafka_broker=args.broker, stream_topic=args.topic)
    sics_client(args.sics, args.port, args.broker, args.topic, state_processor)

#!/usr/bin/python
"""
  The program listens to a ZeroMQ stream and simply forwards the
  {State, Status} messages to the StateProcessor or the {Value}
  messages to the UnitManager objects.
"""
import os
import sys
import argparse
import json
import zmq
import warnings
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from sicsclient.kafkahelp import KafkaLogger
from sicsclient.helpers import get_module_logger
from sicsclient.state import StateProcessor
from sicsclient.units import UnitManager

logger = get_module_logger(__name__)

def open_connection(sics, port):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://{}:{}'.format(sics, port))

    # wait indefinitely for message
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    logger.info("Collecting updates from SICS at {}".format(sics)) 
    return socket   

def sics_client(sics, port, state_processor, unit_manager):

    socket = open_connection(sics, port)

    while True:
        try:
            message = socket.recv().decode('utf-8')
            response = json.loads(message)
            logger.debug("{:.4f}: {}: {}: {}".format(
                response["ts"], response["type"], response["name"], response['value']))
            if response["type"] == "Value":
                unit_manager.new_value_event(response)
            elif state_processor and response["type"] in ["State", "Status"]:
                state_processor.add_event(response)
            else:
                logger.warning("Unsupported: {}".format(str(response)))
        except Exception as e:
            logger.error(str(e))
            time.sleep(1)
            # need to restablish connection - shutdown first and reconnect
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()
            time.sleep(1)
            socket = open_connection(sics, port)

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
        '--base', help='Base command file format', default='./config/base_file.json')
    parser.add_argument(
        '--loglevel', help='Logging level, default=WARNING', default='warning')
    args = parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)
    logger.setLevel(numeric_level)

    unit_manager = UnitManager(args.broker)
    state_processor = StateProcessor(
        args.sics, args.xport, args.base, unit_manager, kafka_broker=args.broker, stream_topic=args.topic)
    sics_client(args.sics, args.port, state_processor, unit_manager)

"""
  The program listens to a ZeroMQ stream and simply forwards all the 
  messages to kafka sics-stream and the {State, Status} messages to 
  the StateProcessor.
"""
from logging import Logger
import os
import sys
import argparse
import logging

from sicsclient.helpers import setup_module_logger, setup_logger_level
from sicsclient.writerstatus import WriterStatus
from sicsclient.state import StateProcessor
from sicsclient.forwarder import Forwarder

sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.realpath(__file__))))

logger = setup_module_logger('sk.launcher')


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
        '--stream', help='Publish to Kafka topic', default='sics-stream')
    parser.add_argument(
        '--nxs_command', help='Writer command topic', default='writer_command')
    parser.add_argument(
        '--nxs_status', help='Writer status topic', default='writer_status')
    parser.add_argument(
        '--base', help='Base command file format', default='./config/base_file.json')
    parser.add_argument(
        '--loglevel', help='Logging level, default=WARNING', default='warning')
    args = parser.parse_args()

    # Set the logger to the appropriate level
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)
    logger.setLevel(numeric_level)
    setup_logger_level('sk.writer', numeric_level)
    setup_logger_level('sk.state', numeric_level)
    setup_logger_level('sk.forwarder', numeric_level)

    # Finally launch the main components
    try:
        writer_status = WriterStatus(args.broker, args.nxs_status)
        state_processor = StateProcessor(
            args.sics, args.xport, args.base, writer_status, kafka_broker=args.broker,
            stream_topic=args.stream, writer_topic=args.nxs_command)
        forwarder = Forwarder(
            args.sics, args.port, state_processor, args.broker, sics_topic=args.stream)
        forwarder.run()

    except KeyboardInterrupt:
        logger.info("%% Aborted by user")

    finally:
        forwarder.close_sics_channel()

#!/usr/bin/python

"""
Tests the forwarding by publishing the messages to a ZeroMQ port which the forwarder is subscribed to 
and confirming that the messages are recovered from Kafka
"""

import zmq
import time
import json
import unittest
import threading

from sicsclient.forwarder import Forwarder
from sicsclient.state import StateProcessor
from sicsclient.writerstatus import WriterStatus

from test.help_tests import extract_f142_data
from kafka import KafkaConsumer
# from streaming_data_types.logdata_f142 import (
#     LogData, Int, Double, String, Value)

Msgs = [
    b'{ "type": "Value", "name": "instrument\\/detector\\/detector_x\\/ignorefault", "value": 0, "seq": 572, "ts": 1585519280.073885 }',
    b'{ "type": "Value", "name": "instrument\\/source\\/cg123", "value": 0, "seq": 573, "ts": 1585519280.075662 }',
    b'{ "type": "Value", "name": "instrument\\/source\\/cns_out", "value": 0, "seq": 574, "ts": 1585519280.077599 }',
    b'{ "type": "Value", "name": "instrument\\/source\\/hb1", "value": 0, "seq": 575, "ts": 1585519281.052323 }',
    b'{ "type": "Value", "name": "instrument\\/source\\/hb2", "value": 12, "seq": 576, "ts": 1585519281.052672 }',
    b'{ "type": "Value", "name": "instrument\\/source\\/power", "value": 20.0, "seq": 577, "ts": 1585519281.052831 }',
    b'{ "type": "Value", "name": "instrument\\/source\\/status", "value": "UNKNOWN", "seq": 578, "ts": 1585519281.052963 }',
    b'{ "type": "Value", "name": "instrument\\/source\\/tg123", "value": 0, "seq": 579, "ts": 1585519281.053126 }',
    b'{ "type": "Value", "name": "instrument\\/source\\/tg4", "value": "UNKNOWN", "seq": 580, "ts": 1585519281.054758 }',
    b'{ "type": "Value", "name": "instrument\\/parameters\\/BeamCenterX", "value": -0.5, "seq": 581, "ts": 1585519281.055728 }',
    b'{ "type": "Value", "name": "instrument\\/parameters\\/BeamCenterZ", "value": 0.5, "seq": 582, "ts": 1585519281.056440 }',
    b'{ "type": "Value", "name": "instrument\\/parameters\\/BeamStop", "value": 100.2, "seq": 583, "ts": 1585519281.057044 }',
    b'{ "type": "Value", "name": "instrument\\/parameters\\/BSdiam", "value": 12.5, "seq": 584, "ts": 1585519281.057651 }',
]


class Publisher:

    def __init__(self, port):
        self.port = port
        self.open_connection()

    def zmq_failure(self):
        '''
        On a failure it will close the zmq socket it will clean up and pause
        '''
        if self.zcontext:
            self.zcontext.destroy(linger=0)
        self.zsock = self.zcontext = None
        time.sleep(1)

    def open_connection(self):
        '''
        Create a new socket connection and register for polling
        '''
        # open the ZeroMQ message
        self.zcontext = zmq.Context()
        self.zsock = self.zcontext.socket(zmq.PUB)
        address = "tcp://*:{:d}".format(self.port)
        self.zsock.bind(address)
        time.sleep(1)
        print('Publisher ready')

    def publish(self, msgs, sleep=1):

        for msg in msgs:
            if sleep:
                time.sleep(sleep)
            self.zsock.send(msg)
            print(msg.decode('utf-8'))


Handlers = {'f142': extract_f142_data}


def extract_data(msg):

    mid = msg.value[4:8].decode('utf-8')
    try:
        return Handlers[mid](msg)
    except KeyError:
        return {}


class TestForwarding(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        # launch the sics client to process the messages
        # - it exists for the duration of the tests
        cls.broker = 'localhost:9092'
        cls.sics_url = 'localhost'
        cls.port = 5566
        cls.topic = 'TEST-sics'
        cls.xport = 5555
        cls.base_file = './config/pln_base.json'
        cls.writer_status = WriterStatus(cls.broker, 'TEST_writerStatus')
        cls.state_processor = StateProcessor(
            cls.sics_url, cls.xport, cls.base_file, cls.writer_status, kafka_broker=cls.broker, stream_topic=cls.topic, timeout_secs=1000)
        forwarder = Forwarder(cls.sics_url, cls.port, cls.state_processor, cls.broker, cls.topic)
        cls.sics = threading.Thread(target=forwarder.run, args=(), daemon=True)
        cls.sics.start()

    def confirm_messages(self, consumer):
        # confirm all the messages are recovered from kafka but ignore
        # status messages that may be interleaved in the kafka stream
        ix = 0
        for msg in consumer:
            rsp = extract_data(msg)
            if rsp['name'] == 'Status':
                continue
            ref = json.loads(Msgs[ix].decode('utf-8'))
            self.assertAlmostEqual(rsp['ts'], ref['ts'], 7)
            self.assertEqual(rsp['name'], ref['name'])
            if isinstance(ref['value'], float):
                self.assertAlmostEqual(rsp['value'], ref['value'], 7)
            else:
                self.assertEqual(rsp['value'], ref['value'])
            ix += 1
        self.assertEqual(ix, len(Msgs))

    def test_forwarding(self):

        publisher = Publisher(self.port)

        # launch the consumer that only listens for lastest messages
        # flush the messages by reading until tmeout
        consumer = KafkaConsumer(self.topic, bootstrap_servers=self.broker, group_id='unit_test',
                                 auto_offset_reset='latest', enable_auto_commit=True, consumer_timeout_ms=5000)
        for msg in consumer:
            pass

        # send all the messages via a different thread
        def send_messages(msgs):
            publisher.publish(msgs, sleep=1)

        sender = threading.Thread(
            target=send_messages, args=(Msgs,), daemon=True)
        sender.start()

        # confirm all the messages are recovered from kafka
        # time.sleep(1000)
        self.confirm_messages(consumer)

    def test_zmq_recovery(self):
        '''
        Disrupt the zmq channel and restore to see if it recovers
        '''
        publisher = Publisher(self.port)

        # launch the consumer that only listens for lastest messages
        # flush the messages by reading until tmeout
        consumer = KafkaConsumer(self.topic, bootstrap_servers=self.broker, group_id='unit_test',
                                 auto_offset_reset='latest', enable_auto_commit=True, consumer_timeout_ms=5000)
        for msg in consumer:
            pass

        # send all the messages via a different thread
        def send_messages(msgs):
            publisher.publish(msgs, sleep=1)

        # send half the messages, break the zmq channel and re-establish
        sender = threading.Thread(
            target=send_messages, args=(Msgs[:6],), daemon=True)
        sender.start()
        sender.join()
        publisher.zmq_failure()
        print('ZMQ publish killed ...')
        time.sleep(10)
        publisher.open_connection()
        sender = threading.Thread(
            target=send_messages, args=(Msgs[6:],), daemon=True)
        sender.start()

        # confirm all the messages are recovered from kafka
        self.confirm_messages(consumer)


if __name__ == '__main__':
    unittest.main()

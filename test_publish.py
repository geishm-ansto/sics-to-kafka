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

from forward import sics_client
from kafka import KafkaConsumer

from pyschema.Int import Int
from pyschema.Double import Double
from pyschema.String import String
from pyschema.LogData import LogData
from pyschema.Value import Value

Msgs = [
    b'{ "type": "Value", "name": "\\/instrument\\/detector\\/detector_x\\/ignorefault", "value": 0, "seq": 572, "ts": 1585519280.073885 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/cg123", "value": 0, "seq": 573, "ts": 1585519280.075662 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/cns_out", "value": 0, "seq": 574, "ts": 1585519280.077599 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/hb1", "value": 0, "seq": 575, "ts": 1585519281.052323 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/hb2", "value": 12, "seq": 576, "ts": 1585519281.052672 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/power", "value": 20.0, "seq": 577, "ts": 1585519281.052831 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/status", "value": "UNKNOWN", "seq": 578, "ts": 1585519281.052963 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/tg123", "value": 0, "seq": 579, "ts": 1585519281.053126 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/tg4", "value": "UNKNOWN", "seq": 580, "ts": 1585519281.054758 }',
    b'{ "type": "Value", "name": "\\/instrument\\/parameters\\/BeamCenterX", "value": -0.5, "seq": 581, "ts": 1585519281.055728 }',
    b'{ "type": "Value", "name": "\\/instrument\\/parameters\\/BeamCenterZ", "value": 0.5, "seq": 582, "ts": 1585519281.056440 }',
    b'{ "type": "Value", "name": "\\/instrument\\/parameters\\/BeamStop", "value": 100.2, "seq": 583, "ts": 1585519281.057044 }',
    b'{ "type": "Value", "name": "\\/instrument\\/parameters\\/BSdiam", "value": 12.5, "seq": 584, "ts": 1585519281.057651 }',
]


class Publisher:

    def __init__(self, port):
        context = zmq.Context()
        self.socket = context.socket(zmq.PUB)
        address = "tcp://*:{:d}".format(port)
        self.socket.bind(address)
        print('Publisher ready')

    def publish(self, msgs, sleep=1):

        for msg in msgs:
            if sleep:
                time.sleep(sleep)
            self.socket.send(msg)
            print(msg.decode('utf-8'))


def extract_f142_data(msg):

    log = LogData.GetRootAsLogData(bytearray(msg.value), 0)

    response = {}
    response['name'] = log.SourceName().decode('utf-8')
    response['ts'] = log.Timestamp() / 1.0e9    # convert to seconds

    value_type = log.ValueType()
    if value_type == Value.Int:
        _val = Int()
        _val.Init(log.Value().Bytes, log.Value().Pos)
        response['value'] = _val.Value()
    elif value_type == Value.Double:
        _val = Double()
        _val.Init(log.Value().Bytes, log.Value().Pos)
        response['value'] = _val.Value()
    elif value_type == Value.String:
        _val = String()
        _val.Init(log.Value().Bytes, log.Value().Pos)
        response['value'] = _val.Value().decode('utf-8')

    return response


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
        cls.sics = 'localhost:5566'
        cls.topic = 'test_sics_stream'
        cls.port = 5566
        sics = threading.Thread(target=sics_client, args=(
            cls.sics, cls.broker, cls.topic), daemon=True)
        sics.start()

    def test_forwarding(self):

        publisher = Publisher(self.port)

        # launch the consumer that only listens for lastest messages
        consumer = KafkaConsumer(self.topic, bootstrap_servers=self.broker, group_id='unit_test',
                                 auto_offset_reset='latest', enable_auto_commit=True, consumer_timeout_ms=2000)

        # send all the messages via a different thread
        def send_messages(msgs):
            publisher.publish(msgs, sleep=1)

        sender = threading.Thread(
            target=send_messages, args=(Msgs,), daemon=True)
        sender.start()

        # confirm all the messages are recovered from kafka
        ix = 0
        for msg in consumer:
            rsp = extract_data(msg)
            ref = json.loads(Msgs[ix].decode('utf-8'))
            self.assertAlmostEqual(rsp['ts'], ref['ts'], 7)
            self.assertEqual(rsp['name'], ref['name'])
            if isinstance(ref['value'], float):
                self.assertAlmostEqual(rsp['value'], ref['value'], 7)
            else:
                self.assertEqual(rsp['value'], ref['value'])
            ix += 1
        self.assertEqual(ix, len(Msgs))


if __name__ == '__main__':
    unittest.main()

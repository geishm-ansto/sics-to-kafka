#!/usr/bin/python

"""
Generate a repetitive signal throu a zeroMQ socket
"""

import zmq
import time
import argparse

Msgs = [
    b'{ "type": "Value", "name": "\\/instrument\\/detector\\/detector_x\\/ignorefault", "value": "0", "seq": 572, "ts": 1585519280.073885 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/cg123", "value": "0", "seq": 573, "ts": 1585519280.075662 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/cns_out", "value": "0", "seq": 574, "ts": 1585519280.077599 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/hb1", "value": "0", "seq": 575, "ts": 1585519281.052323 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/hb2", "value": "0", "seq": 576, "ts": 1585519281.052672 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/power", "value": "0", "seq": 577, "ts": 1585519281.052831 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/status", "value": "UNKNOWN", "seq": 578, "ts": 1585519281.052963 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/tg123", "value": "0", "seq": 579, "ts": 1585519281.053126 }',
    b'{ "type": "Value", "name": "\\/instrument\\/source\\/tg4", "value": "UNKNOWN", "seq": 580, "ts": 1585519281.054758 }',
    b'{ "type": "Value", "name": "\\/instrument\\/parameters\\/BeamCenterX", "value": "0", "seq": 581, "ts": 1585519281.055728 }',
    b'{ "type": "Value", "name": "\\/instrument\\/parameters\\/BeamCenterZ", "value": "0", "seq": 582, "ts": 1585519281.056440 }',
    b'{ "type": "Value", "name": "\\/instrument\\/parameters\\/BeamStop", "value": "0", "seq": 583, "ts": 1585519281.057044 }',
    b'{ "type": "Value", "name": "\\/instrument\\/parameters\\/BSdiam", "value": "0", "seq": 584, "ts": 1585519281.057651 }',
]

def publish(args):

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    address = "tcp://*:{:d}".format(args.port)
    socket.bind(address)

    while True:
        for msg in Msgs:
            print(msg.decode('utf-8'))
            socket.send(msg)
            time.sleep(args.sleep)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Publish ZroMQ messages on port')
    parser.add_argument('--port', help='publish on port', default=5556)
    parser.add_argument('--sleep', help='secs between messages', default=2)
    parser.add_argument('--topic', help='Publish to Kafka topic', default='sics_stream')
    args = parser.parse_args()

    publish(args)
import numpy as np

from kafka import KafkaProducer

from streaming_data_types.logdata_f142 import (serialise_f142, _serialise_ubytearray,
                                               _setup_builder, _complete_buffer)
from streaming_data_types.run_start_pl72 import deserialise_pl72, serialise_pl72
from streaming_data_types.run_stop_6s4t import deserialise_6s4t, serialise_6s4t

from sicsclient.helpers import setup_module_logger

logger = setup_module_logger(__name__)


def timestamp_to_nsecs(ts):
    return int(ts * 1e9)


def timestamp_to_msecs(ts):
    return int(ts * 1e3)

def create_f142_message(timestamp, tag, value):

    timestamp_ns = timestamp_to_nsecs(timestamp)
    if type(value) == str:
        # overrride the current implementation as it appears wrong, 
        # it converts a ubyte array to a ushort array
        bvalue = np.array(list(value.encode('utf-8')), dtype=np.ubyte)
        builder, source = _setup_builder(tag)
        _serialise_ubytearray(builder, bvalue, source)
        buff = _complete_buffer(builder, timestamp_ns, )

    else:
        buff = serialise_f142(value, tag, timestamp_ns)
    return buff

def publish_f142_message(producer, topic, timestamp_sec, tag, value):
    """
    Create, publish and flush an f142 message to a given topic.
    """
    msg = create_f142_message(timestamp_sec, tag, value)
    producer.send(
        topic, msg, timestamp_ms=timestamp_to_msecs(timestamp_sec))
    producer.flush()
class KafkaLogger(object):

    def __init__(self, broker):
        try:
            self.producer = KafkaProducer(bootstrap_servers=[broker])
        except AttributeError:
            raise ValueError(
                'Failed to create kafka producer: {}'.format(broker))

    def publish_f142_message(self, topic, timestamp, tag, value):
        publish_f142_message(self.producer, topic, timestamp, tag, value)


def create_runstart_message(cmdargs):
    """
    Convert the command arguments to a flatbuffer nexus writer message
    """
    return serialise_pl72(**cmdargs)


def create_runend_message(stop_time, job_id):
    return serialise_6s4t(job_id=job_id, stop_time=stop_time)


def extract_runstart_data(buffer):

    return deserialise_pl72(buffer)


def extract_runstop_data(buffer):

    return deserialise_6s4t(buffer)


def publish_message(producer, topic, timestamp_msec, msg):
    """
    Send and flush the message to a given topic.
    """
    # send and flush so that messages are not batched
    producer.send(
        topic, value=msg, timestamp_ms=timestamp_msec)
    producer.flush()


def get_kafka_tag_value(sics_msg):
    msg_type = sics_msg['type'].lower()
    if msg_type == 'value':
        # drop the leading '/'
        if sics_msg['name'][0] == '/':
            tag = sics_msg['name'][1:]
        else:
            tag = sics_msg['name']
        value = sics_msg['value']
    else:
        tag = msg_type
        value = '{}: {}'.format(sics_msg['name'], sics_msg['value'])
    return tag, value

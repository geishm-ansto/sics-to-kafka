import numpy as np
import json
import time

from streaming_data_types.status_x5f2 import serialise_x5f2
from streaming_data_types.logdata_f142 import deserialise_f142
from sicsclient.cmdbuilder import CommandBuilder
from sicsclient.kafkahelp import (KafkaProducer, timestamp_to_msecs,
                                  create_runstart_message, publish_message)


def extract_f142_data(msg):

    dc = deserialise_f142(bytearray(msg.value))

    response = {}
    response['name'] = dc.source_name
    response['ts'] = dc.timestamp_unix_ns / 1e9     # nsec to sec
    try:
        is_string = (dc.value.dtype == np.ubyte)
    except AttributeError:
        is_string = False

    if is_string:
        response['value'] = dc.value.tostring().decode('utf-8')
    else:
        response['value'] = dc.value

    return response


def send_write_command(filename):
    timestamp_ms = timestamp_to_msecs(time.time())
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = 'TEST_writerCommand'
    cbld = CommandBuilder(filename)
    msg = create_runstart_message(cbld.get_command())
    publish_message(producer, topic, timestamp_ms, msg)


def create_x5f2_status(json_content):
    cmdargs = {
        'software_name': "UnitTest",
        'software_version': "1.0",
        'service_id': "Mocked",
        'host_name': "localhost",
        'process_id': 1234,
        'update_interval': 0
    }
    cmdargs['status_json'] = json.dumps(json_content)
    return serialise_x5f2(**cmdargs)



import json
import time

from sicsclient.pyschema.JsonData import JsonData
from sicsclient.pyschema.LogData import LogData
from sicsclient.pyschema.Int import Int
from sicsclient.pyschema.Double import Double
from sicsclient.pyschema.String import String
from sicsclient.pyschema.Value import Value

from sicsclient.cmdbuilder import CommandBuilder
from sicsclient.kafkahelp import (KafkaProducer, timestamp_to_msecs,
                                  create_runstart_message, publish_message)

def extract_json_data(msg):
    jmsg = JsonData.GetRootAsJsonData(bytearray(msg.value), 0)
    jstr = jmsg.Json().decode('utf-8')
    return json.loads(jstr)

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


Handlers = {'f142': extract_f142_data,
            'json': extract_json_data}


def extract_data(msg):

    mid = msg.value[4:8].decode('utf-8')
    try:
        return Handlers[mid](msg)
    except KeyError:
        return {}

def send_write_command(filename):
    timestamp_ms = timestamp_to_msecs(time.time())
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = 'TEST_writerCommand'
    cbld = CommandBuilder(filename)
    msg = create_runstart_message(cbld.get_command())
    publish_message(producer, topic, timestamp_ms, msg)

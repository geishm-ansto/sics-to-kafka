
import flatbuffers
import json
import time

from streaming_data_types.status_x5f2 import Status
from streaming_data_types.logdata_f142 import (
    LogData, Int, Double, String, Value)

from sicsclient.cmdbuilder import CommandBuilder
from sicsclient.kafkahelp import (KafkaProducer, timestamp_to_msecs,
                                  create_runstart_message, publish_message)


def extract_f142_data(msg):

    log = LogData.LogData.GetRootAsLogData(bytearray(msg.value), 0)

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


def send_write_command(filename):
    timestamp_ms = timestamp_to_msecs(time.time())
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = 'TEST_writerCommand'
    cbld = CommandBuilder(filename)
    msg = create_runstart_message(cbld.get_command())
    publish_message(producer, topic, timestamp_ms, msg)


def create_x5f2_status(json_content):
    file_identifier = b"x5f2"
    builder = flatbuffers.Builder(1024)
    software_str = builder.CreateString("UnitTest")
    version_str = builder.CreateString("1.0")
    service_str = builder.CreateString("Mocked")
    host_str = builder.CreateString("localhost")
    json_str = builder.CreateString(json.dumps(json_content).encode('utf-8'))
    Status.StatusStart(builder)
    Status.StatusAddServiceId(builder, software_str)
    Status.StatusAddSoftwareVersion(builder, version_str)
    Status.StatusAddServiceId(builder, service_str)
    Status.StatusAddHostName(builder, host_str)
    Status.StatusAddStatusJson(builder, json_str)
    msg = Status.StatusEnd(builder)
    builder.Finish(msg)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return bytes(buff)

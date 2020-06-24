import flatbuffers
from kafka import KafkaProducer

import sicsclient.pyschema.JsonData as JsonData
import sicsclient.pyschema.LogData as LogData
from sicsclient.pyschema.Value import Value
from sicsclient.pyschema.Int import IntStart, IntAddValue, IntEnd
from sicsclient.pyschema.Double import DoubleStart, DoubleAddValue, DoubleEnd
from sicsclient.pyschema.String import StringStart, StringAddValue, StringEnd

from sicsclient.helpers import get_module_logger

logger = get_module_logger(__name__)

def timestamp_to_nsecs(ts):
    return int(ts * 1e9)


def timestamp_to_msecs(ts):
    return int(ts * 1e3)


def add_int_value(builder, value):
    IntStart(builder)
    IntAddValue(builder, value)
    position = IntEnd(builder)
    return position, Value.Int


def add_float_value(builder, value):
    DoubleStart(builder)
    DoubleAddValue(builder, value)
    position = DoubleEnd(builder)
    return position, Value.Double


def add_string_value(builder, value):
    svalue = builder.CreateString(value)
    StringStart(builder)
    StringAddValue(builder, svalue)
    position = StringEnd(builder)
    return position, Value.String


MapValue = {
    int: add_int_value,
    float: add_float_value,
    str: add_string_value,
}


class KafkaLogger(object):

    def __init__(self, broker):
        try:
            self.producer = KafkaProducer(bootstrap_servers=[broker])
        except AttributeError:
            raise ValueError(
                'Failed to create kafka producer: {}'.format(broker))

    def create_f142_message(self, timestamp, tag, value):

        file_identifier = b"f142"
        builder = flatbuffers.Builder(1024)
        source = builder.CreateString(tag)
        try:
            (posn, val_type) = MapValue[type(value)](builder, value)
        except (KeyError):
            logger.warning('No suitable builder for type {}'.format(type(value)))
            return None

        # Build the actual buffer
        LogData.LogDataStart(builder)
        LogData.LogDataAddSourceName(builder, source)
        LogData.LogDataAddValue(builder, posn)
        LogData.LogDataAddValueType(builder, val_type)
        LogData.LogDataAddTimestamp(builder, timestamp_to_nsecs(timestamp))
        log_msg = LogData.LogDataEnd(builder)
        builder.Finish(log_msg)

        # Generate the output and replace the file_identifier
        buff = builder.Output()
        buff[4:8] = file_identifier
        return bytes(buff)

    def publish_f142_message(self, topic, timestamp, tag, value):
        """
        Publish an f142 message to a given topic.
        """
        msg = self.create_f142_message(timestamp, tag, value)
        if msg:
            # send and flush so that messages are not batched
            self.producer.send(
                topic, msg, timestamp_ms=timestamp_to_msecs(timestamp))
            self.producer.flush()

    def create_json_message(self, timestamp, jstr):
        file_identifier = b"json"
        builder = flatbuffers.Builder(1024)
        body = builder.CreateString(jstr)    
        JsonData.JsonDataStart(builder)
        JsonData.JsonDataAddJson(builder, body)
        msg = JsonData.JsonDataEnd(builder)
        builder.Finish(msg)  

        # Generate the output and replace the file_identifier
        buff = builder.Output()
        buff[4:8] = file_identifier
        return bytes(buff)

    def publish_json_message(self, topic, timestamp, jstr):
        """
        Publish a json message to a given topic.
        """
        msg = self.create_json_message(timestamp, jstr)
        if msg:
            # send and flush so that messages are not batched
            self.producer.send(
                topic, msg, timestamp_ms=timestamp_to_msecs(timestamp))
            self.producer.flush()
import json
import time

from sicsclient.cmdbuilder import CommandBuilder
from sicsclient.kafkahelp import (KafkaProducer, timestamp_to_msecs,
                                  create_runstart_message, publish_message)


def send_write_command(filename):
    timestamp_ms = timestamp_to_msecs(time.time())
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = 'TEST_writerCommand'
    cbld = CommandBuilder(filename)
    msg = create_runstart_message(cbld.get_command())
    publish_message(producer, topic, timestamp_ms, msg)

if __name__ == '__main__':
    send_write_command('./test/data/temp.json')
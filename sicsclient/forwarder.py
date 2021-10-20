#!/usr/bin/python
"""
  The program listens to a ZeroMQ stream and simply forwards all the 
  messages to kafka sics-stream and the {State, Status} messages to 
  the StateProcessor.
"""
import json
import kafka
import zmq
import time
from datetime import datetime

from sicsclient.helpers import unix_time_milliseconds
from sicsclient.kafkahelp import KafkaLogger, get_kafka_tag_value, setup_module_logger


logger = setup_module_logger('sk.forwarder', 'DEBUG')


class Forwarder:
    def __init__(self, sics_ip, sics_port, state_processor, kafka_broker, sics_topic) -> None:

        self._sics_ip = sics_ip
        self._sics_port = sics_port
        self._state_processor = state_processor
        self._kafka_broker = kafka_broker
        self._sics_topic = sics_topic

    def open_sics_channel(self):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        self._socket.connect(f"tcp://{self._sics_ip}:{self._sics_port}")
        # wait indefinitely for message
        self._socket.setsockopt(zmq.SUBSCRIBE, b"")
        logger.info(
            f"Opened SICS connection: {self._sics_ip}:{self._sics_port}")

    def close_sics_channel(self):
        if self._context:
            self.context.destroy(linger=0)
            self.context = self._socket = None
            logger.info(
                f"Closed SICS connection: {self._sics_ip}:{self._sics_port}")

    def run(self):
        """
        Runs forever and traps zmq or kafka erors and tries to recover. All other
        exceptions pass through.
        """
        last_seqn = None
        self.open_sics_channel()
        kafka_logger = KafkaLogger(self._kafka_broker)
        while True:
            try:
                message = self._socket.recv().decode('utf-8')
                response = json.loads(message)
                logger.debug("{:.4f}: {}: {}: {}".format(
                    response["ts"], response["type"], response["name"], response['value']))

                # log missing messages as a status warning to kafka
                curr_seqn = int(response["seq"])
                if last_seqn and (curr_seqn != last_seqn + 1):
                    tag = 'Status'
                    value = 'Warning: Dropped {} SICS messages'.format(
                        curr_seqn - last_seqn - 1)
                    ts_secs = unix_time_milliseconds(
                        datetime.utcnow()) / 1000.0
                    kafka_logger.publish_f142_message(
                        self._sics_topic, ts_secs, tag, value)
                    logger.debug("{:.4f}: {}: {}: {}".format(
                        ts_secs, 'Status', tag, value))
                last_seqn = curr_seqn

                if response["type"] in ["State", "Status"]:
                    self._state_processor.add_event(response)

                # copy the message to the sics stream
                event_ts = response["ts"]
                tag, value = get_kafka_tag_value(response)
                kafka_logger.publish_f142_message(
                    self._sics_topic, event_ts, tag, value)

            except (KeyError, AttributeError):
                logger.error('SICS message has missing elements')
                continue

            except kafka.errors.KafkaError:
                kafka_logger = KafkaLogger(self._kafka_broker)
                time.sleep(1)

            except zmq.error.ZMQError as e:
                logger.error(str(e))
                self.close_sics_channel()
                time.sleep(1)
                self.open_sics_channel()



#!/usr/bin/python

"""
Basic tests for the nexus writer status monitor.
"""

import time
from types import SimpleNamespace
import unittest

from unittest.mock import patch, Mock
from sicsclient.writerstatus import WriterStatus
from test.help_tests import create_x5f2_status


c_mybroker = 'MyBroker'
c_mytopic = 'status-topic'


def my_json_loads(value):
    return {'job_id': ''}


def return_empty():
    obj = create_x5f2_status({'job_id': ''})
    msg = SimpleNamespace(value=obj)
    return {1: [msg]}


def return_busy():
    obj = create_x5f2_status({'job_id': 'abcd'})
    msg = SimpleNamespace(value=obj)
    return {1: [msg]}


class MyConsumer(Mock):
    invoked = 0
    blocked = False

    def __iter__(self):
        self._mock = Mock()
        self.invoked = 0
        return self

    def __next__(self):
        self.invoked += 1
        if self.invoked < 5:
            return return_busy()
        else:
            return return_empty()

    def poll(self, timeout_ms=0):
        if self.blocked:
            time.sleep(timeout_ms / 1000.)
            return None
        else:
            time.sleep(1)
            return self.__next__()


class TestWriterStats(unittest.TestCase):

    # Confirm that the consumer access the correct broker and topic
    @patch('sicsclient.writerstatus.loads', my_json_loads)
    @patch('sicsclient.writerstatus.KafkaConsumer')
    @patch('sicsclient.writerstatus.bytearray')
    @patch('sicsclient.writerstatus.x5f2.Status.GetRootAsStatus')
    def test_broker_topic(self, x5f2_msg, m_bytearray, consumer):
        WriterStatus(c_mybroker, c_mytopic)
        consumer.assert_called_once()
        _, kwargs = consumer.call_args
        self.assertTrue('bootstrap_servers' in kwargs)
        self.assertEqual(kwargs['bootstrap_servers'], c_mybroker)
        consumer().subscribe.assert_called_once_with([c_mytopic])

    # Confirm that it regularly polls the kafka consumer
    # by mocking the call to the kafka consumer

    @patch('sicsclient.writerstatus.KafkaConsumer', MyConsumer)
    def test_regular_request(self):

        writer = WriterStatus(c_mybroker, c_mytopic)
        time.sleep(5.5)
        self.assertGreaterEqual(writer.consumer.invoked, 5)
        job = writer.get_active_job()
        self.assertEqual(job, "")
        ok = writer.wait_for_idle(5)
        self.assertTrue(ok)

    # Confirm that the wait for idle blocks while it is busy
    @patch('sicsclient.writerstatus.KafkaConsumer', MyConsumer)
    def test_blocking_wait(self):
        writer = WriterStatus(c_mybroker, c_mytopic)
        time.sleep(2)
        job = writer.get_active_job()
        self.assertEqual(job, "abcd")
        ok = writer.wait_for_idle(1)
        self.assertFalse(ok)
        ok = writer.wait_for_idle(5)
        self.assertTrue(ok)
        job = writer.get_active_job()
        self.assertEqual(job, "")
        ok = writer.wait_for_idle(0.1)
        self.assertTrue(ok)


if __name__ == '__main__':
    unittest.main()

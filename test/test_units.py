#!/usr/bin/python

"""
Tests that the units manager captures the values and units and regularly 
updates the kafka server with the units. The kakfka server is assumed to be available on
'localhost:9092'
"""

import time
import json
import unittest

from kafka import KafkaConsumer
from sicsclient.units import UnitManager, Parameter
from sicsclient.kafkahelp import timestamp_to_msecs
from help_tests import extract_data

kafka_broker = 'localhost:9092'
ValueTopic = 'TEST_sics_stream'
UnitTopic = 'TEST_sics_units'


def create_consumer(topic, timeout_ms=5000):
    # launch the consumer that only listens for lastest messages and flush the entries
    consumer = KafkaConsumer(topic, bootstrap_servers=kafka_broker, group_id=None,
                             auto_offset_reset='latest', enable_auto_commit=True, consumer_timeout_ms=timeout_ms)
    time.sleep(1)
    print('flush entries ..')
    for msg in consumer:
        print('  ts: {}'.format(msg.timestamp))
    return consumer


class TestUnits(unittest.TestCase):

    unit_values = {
        '/monitor/bm1_counts': Parameter(1234, 'count'),
        '/monitor/bm1_event_rate': Parameter(12.34, 'count/sec'),
        '/instrument/aperture/sh1': Parameter(10.0, 'mm'),
        '/instrument/detector/tofw': Parameter(100.5, 'microseconds'),
        '/instrument/detector/total_counts': Parameter(123400, 'count'),
        '/sample/bsr': Parameter(22.5, 'degC')
    }

    values = [
        b'{ "type": "Value", "name": "\\/monitor\\/bm1_counts", "value": 3456, "seq": 572, "ts": 1585519280.073885 }',
        b'{ "type": "Value", "name": "\\/monitor\\/bm1_event_rate", "value": 10.0, "seq": 573, "ts": 1585519281.073885 }',
    ]

    def confirm_unit_values(self, kafka_message, ref_values):
        '''
        Confirms that the unit and values match the table.
        '''
        # confirm all the messages are recovered from kafka
        ix = 0
        uvalues = extract_data(kafka_message)
        for tag, pm in uvalues.items():
            value, unit = pm
            rp = ref_values[tag]
            self.assertAlmostEqual(value, rp.value, 7)
            self.assertEqual(unit, rp.unit)
            ix += 1
        self.assertEqual(ix, len(ref_values))

    @classmethod
    def setUpClass(self):
        TestUnits.unm = UnitManager(kafka_broker, log_period_secs=5,
                          unit_topic=UnitTopic, value_topic=ValueTopic)

    def test_value_updated(self):
        '''
        Confirms that the value is sent to the kafka topic with the correct 
        value and timestamp 
        '''
        print('\ntest_value_updated::')
        # launch the consumer that only listens for lastest messages
        consumer = create_consumer(ValueTopic, timeout_ms=1000)
        TestUnits.unm.reset_test()
        TestUnits.unm.set_log_data(False)
        time.sleep(1)

        # send the messages and confirm they are present in the value topic
        for line in self.values:
            msg = json.loads(line)
            msg['ts'] = time.time()
            TestUnits.unm.new_value_event(msg)

        time.sleep(1)
        ix = 0
        for msg in consumer:
            rsp = extract_data(msg)
            ref = json.loads(self.values[ix].decode('utf-8'))
            #self.assertAlmostEqual(rsp['ts'], ref['ts'], 7)
            self.assertEqual(rsp['name'], ref['name'])
            if isinstance(ref['value'], float):
                self.assertAlmostEqual(rsp['value'], ref['value'], 7)
            else:
                self.assertEqual(rsp['value'], ref['value'])
            ix += 1
        self.assertEqual(ix, len(self.values))

        # and that the value is updated
        for msg in self.values:
            ref = json.loads(msg)
            self.assertEqual(TestUnits.unm.get_parameter(
                ref['name']).value, ref['value'])

        time.sleep(1)

    def test_snapshot(self):
        '''
        Confirm all the values and units are recovered from the kafka topic
        '''
        print('\ntest_snapshot::')
        consumer = create_consumer(UnitTopic, timeout_ms=1000)

        #unm = UnitManager(kafka_broker, log_period_secs=1000,
        #                  unit_topic=UnitTopic, value_topic=ValueTopic)
        TestUnits.unm.reset_test()
        TestUnits.unm.set_log_data(False)
        ts = time.time()
        TestUnits.unm.set_unit_values(ts, unit_values=self.unit_values)
        time.sleep(1)

        ix = 0
        for msg in consumer:
            print('ts: {}, {}'.format(msg.timestamp, timestamp_to_msecs(ts)))
            self.assertEqual(msg.timestamp, timestamp_to_msecs(ts))
            self.confirm_unit_values(msg, self.unit_values)
            ix += 1
        self.assertEqual(ix, 1)

        time.sleep(1)

    def test_log_unit_values(self):
        '''
        Confirm all the values and units are recovered from the kafka topic. 
        The expected messages is 3 because the set call includes a snapshot
        because the unit changed.
        '''
        print('\ntest_unit_values::')
        consumer = create_consumer(UnitTopic, timeout_ms=1000)
        TestUnits.unm.reset_test()
        TestUnits.unm.set_log_data(True)
        ts = time.time()
        TestUnits.unm.set_unit_values(ts, unit_values=self.unit_values)   # recorded
        # not recorded - no unit change
        TestUnits.unm.set_unit_values(ts, unit_values=self.unit_values)
        time.sleep(12)
        TestUnits.unm.set_log_data(False)

        ix = 0
        for msg in consumer:
            print('ts: {}'.format(msg.timestamp))
            self.confirm_unit_values(msg, self.unit_values)
            ix += 1
        self.assertEqual(ix, 3)

        time.sleep(1)


if __name__ == '__main__':
    unittest.main()

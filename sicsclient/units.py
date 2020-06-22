
import time
import json
import threading
from collections import namedtuple
from kafkahelp import KafkaLogger

Parameter = namedtuple('Parameter', ['value', 'unit'])

class UnitManager(object):
    '''
    Maintains a list of units and current values for the SICS parameters.
    It receives 'values' and 'units' asynchronously and manages the logging 
    of these values to kafka topics 'sics-stream' and 'sics-units'. 
    The inputs execute in the caller context while the regular dump runs
    in a separate thread.   
    '''

    def __init__(self, broker, log_period_secs=60,
                 unit_topic='sics-units', value_topic='sics-stream'):

        self.unit_values = {}
        self.log_period = log_period_secs
        self.log_data = True
        self.unit_topic = unit_topic
        self.value_topic = value_topic
        self.kafka = KafkaLogger(broker)
        self.time_offset = 0

        # create the thread that will rebuild the units
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.logging_task, daemon=True)
        self.thread.start()

    def __del__(self):
        del self.kafka
        
    def new_value_event(self, response):
        '''
        Changed value event from SICS.
        . Update local copy
        . Send to 'sics-stream'
        Executes in the caller context. 
        '''
        tag = response['name']
        value = response['value']
        event_ts = response['ts']
        with self.lock:
            try:
                unit = self.unit_values[tag].unit
                self.unit_values[tag] = Parameter(value, unit)
            except KeyError:
                self.unit_values[tag] = Parameter(value, "")

        self.kafka.publish_f142_message(
            self.value_topic, event_ts, tag, value)

    def set_unit_values(self, timestamp, unit_values):
        '''
        Set the units and current value for the tag, if a unit has
        changed dump the unit value table to kafka.
        Executes in the caller context.
        '''
        unit_changed = False
        for tag, pm in unit_values.items():
            with self.lock:
                try:
                    unit = self.unit_values[tag].unit
                except KeyError:
                    unit = ""
                if pm.unit != unit:
                    unit_changed = True
                self.unit_values[tag] = Parameter(pm.value, pm.unit)

        if unit_changed:
            self.snapshot(timestamp)

    def snapshot(self, timestamp):
        '''
        Convert the data to a json string and send to kafka. The timestamp
        is based on the time module which may differ from the time stamp
        from the SICS stream. The time offset in mil-sec records the difference
        between the system clock and the timestamp 
        '''
        with self.lock:
            jvalues = json.dumps(self.unit_values)

        self.kafka.publish_json_message(self.unit_topic, timestamp, jvalues)

    def logging_task(self):
        '''
        Get the current time and if the time period has elapsed:
        . dump all of the values and units to 'sics-units'
        . reset dump time 
        '''
        while True:
            time.sleep(self.log_period)
            if self.log_data:
                snap_time = time.time() + self.time_offset
                self.snapshot(snap_time)

    def get_parameter(self, tag):
        '''
        Returns the (value, unit) for the parameter name.
        '''
        try:
            with self.lock:
                pm = self.unit_values[tag]
        except KeyError:
            pm = None
        return pm

    def set_log_data(self, value):
        '''
        Enable/disable periodic logging of units
        '''
        self.log_data = value

    def synch_time(self, sics_ts_secs):
        '''
        Account for the difference in time between the source
        and local time. Needed to align the periodic units logs
        to use comparable times.
        '''
        clock_ts = time.time()
        self.time_offset = sics_ts_secs - clock_ts

    def reset_test(self):
        '''
        Clears the unit_values
        '''
        with self.lock:
            self.unit_values = {}


"""
Provides a class that manages the state events from the SICS stream. It is used
to issue a write command to the kafka to nexus writer.

"""
import os
import threading
import zmq
import time
import json
import uuid

from queue import Queue
from sicsclient.parsexml import parsesics
from sicsclient.cmdbuilder import CommandBuilder
from sicsclient.kafkahelp import KafkaProducer, timestamp_to_msecs
from sicsclient.units import Parameter
from sicsclient.helpers import get_module_logger

logger = get_module_logger(__name__)

def find_nodes(clist, tag):
    '''
    Finds the list of nodes that match the tag in the Component list.
    '''
    nodes = []
    etag = '/' + tag
    for cmp in clist:
        if cmp.tag.endswith(etag) or cmp.tag == tag:
            nodes.append(cmp)
    return nodes


class StateProcessor(object):
    """
    Implements a psuedo state machine that responds to incoming events (json dictionaries).
    The events are queued and processed in a separate thread. As a single thread completes
    the execution there is no need for a lock to protect the data.
    """

    def __init__(self, sics, port, basefile, unit_manager, kafka_broker='localhost:9092', recv_wait=1000, ident='kafka',
                 stream_topic='sics-stream', writer_topic='TEST_writerCommand'):

        self.cmd_builder = None
        self.basefile = basefile
        self.unit_manager = unit_manager
        self.stream_topic = stream_topic
        self.writer_topic = writer_topic
        self.kafka_producer = KafkaProducer(bootstrap_servers=[kafka_broker])
        self.event_queue = Queue()
        self.recv_wait = recv_wait
        self.end_point = 'tcp://{}:{}'.format(sics, port)
        self.socket = None
        self.transactions = 0
        self.ident = ident
        self.open_connection()

        # create the thread that will rebuild the units
        self.thread = threading.Thread(target=self.process_events, daemon=True)
        self.thread.start()

    def open_connection(self, recover=False):

        if recover and self.socket:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            time.sleep(1)

        # open the ZeroMQ message
        context = zmq.Context()
        self.socket = context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, self.ident.encode('utf-8'))
        self.socket.setsockopt(zmq.RCVTIMEO, self.recv_wait)
        self.socket.connect(self.end_point.encode('utf-8'))
        logger.info('zmq.DEALER {} connection to {}'.format(
            self.ident, self.end_point))

    def add_hdf_component(self, cmp):
        '''
        Add each component to the command builderProcess each component where:
        . 'f142' type {txt,int,flt}, mutable
        . 'dataset' type {txt,int,flt}, not mutable
        '''
        if cmp.mutable:
            atts = [('units', cmp.units)] if cmp.units else []
            self.cmd_builder.add_stream(
                cmp.tag, self.stream_topic, cmp.tag, 'f142', cmp.dtype, atts)
        else:
            atts = [('units', cmp.units)] if cmp.units else []
            self.cmd_builder.add_dataset(cmp.tag, cmp.value, cmp.dtype, atts)

    def get_xml_parameters(self):
        '''
        This will be replaced with the 'getkafkaxml' command when it
        is available - use 'getgumtreexml /' for now.
        . Request the kafka xml description from sics
        . Parse the xml to recover the component list
        '''
        resp = self.cmd_request('SICS', 'getgumtreexml /')
        if resp and resp['flag'].lower() == 'ok':
            clist = parsesics(resp['reply'])
        else:
            logger.warning('Unable to recover gumtreexml')
            self.cmd_builder = None
            clist = []
        return clist

    def start_scan(self, scan_ts_secs):
        '''
        A histmem scan was STARTED:
        . Check if previous command was not completed and log message.
        . Get the kafkaxml structure and parse
        . Build the cmd structure
        . Notify the unit manager of the component list
        '''
        # if a start scan was initiated then raise an alert
        if self.cmd_builder:
            logger.info('Ignoring previous start scan that was not completed!')
        self.cmd_builder = None

        # get the component list form the xml description
        # and recover the key parameters
        clist = self.get_xml_parameters()
        starts = find_nodes(clist, 'start_time')
        start_time = starts[0].value if starts else scan_ts_secs
        start_time_ms = timestamp_to_msecs(start_time)
        stops = find_nodes(clist, 'stop_time')
        stop_time_ms = timestamp_to_msecs(stops[0].value) if stops else None
        fnames = find_nodes(clist, 'file_name')
        if fnames:
            file_name, file_ext = os.path.splitext(fnames[0].value)
            file_name = file_name + '.nxs'
        else:
            logger.warning('Need file name to complete write request')
            return

        # load the default command string for the instrument
        self.cmd_builder = CommandBuilder(self.basefile)
        self.cmd_builder.set_param(
            filename=file_name, start_time_ms=start_time_ms,
            stop_time_ms=stop_time_ms, job_id=str(uuid.uuid4()))

        # finally add the hdf nodes from the component list ad forward the component list
        # to the unit manager
        unit_values = {}
        for cmp in clist:
            self.add_hdf_component(cmp)
            unit_values[cmp.tag] = Parameter(cmp.value, cmp.units)
        self.unit_manager.set_unit_values(start_time, unit_values)

        # command builder may only require the stop time to be updated

    def end_scan(self, finish_ts_secs):
        '''
        Expects to be invoked after a start_scan which setups the 
        command builder. 
        If the stop time is not present it will update with the passed
        timestop as the stop time.
        Finally it issues a write request.
        '''
        if not self.cmd_builder:
            logger.warning('Missing start scan event - do nothing!')
            return

        # if the stop time was not specified then set it
        if not self.cmd_builder.get_stop_time():
            self.cmd_builder.set_param(
                stop_time_ms=timestamp_to_msecs(finish_ts_secs))

        # build the json command and send to the writer command
        json_cmd = self.cmd_builder.as_json()
        self.send_write_cmd(timestamp_to_msecs(finish_ts_secs), json_cmd)

        # For now just issue a write command and forget, if we need a status
        # then the command builder or the job id may need to managed until
        # the write is confirmed. Just delete until more is needed.
        logger.info('Issued write request to the nexus writer topic')
        self.cmd_builder = None

    def send_write_cmd(self, timestamp_ms, json_cmd):
        self.kafka_producer.send(
            self.writer_topic, value=json_cmd, timestamp_ms=timestamp_ms)
        self.kafka_producer.flush()

    def cmd_request(self, cmd, text):

        self.transactions += 1
        request = {
            'trans': self.transactions,
            'cmd': cmd,
            'text': text
        }
        msg = json.dumps(request).encode('utf-8')
        try:
            self.socket.send(msg)
            while True:
                message = self.socket.recv().decode('utf-8')
                if message.startswith("{"):
                    response = json.loads(message)
                else:
                    response = {'reply': "Nothing"}
                if 'final' in response and response['final']:
                    return response
        except Exception as exc:
            # There is a significant risk of a dead lock with the ZMQ client server model
            # so after printing the message re-establish the connection
            logger.error('ZeroMQ: {}'.format(str(exc)))
            logger.info('Re-establishing connection because of the failure...')
            self.open_connection(recover=True)
        return {}

    def process_events(self):
        '''
        Responsible for processing the events in the queue. It looks for 
        one of {STARTED, FINISH} state events. The file writing happens at
        the end of the data collection (FINISH). The process only supports 
        a single write cycle at a time. 
        . on STARTED hmscan create command build object and populate 
          with parameters that need to be saved
        . on FINISH hmscan update the stop time and issue the write command
        '''

        while True:
            # get the message from the queue
            resp = self.event_queue.get()
            if resp is None:
                continue

            # do something useful
            if resp["type"] == "State" and resp["name"] == "STARTED" and resp["value"] == "hmscan":
                self.start_scan(resp["ts"])
            elif resp["type"] == "State" and resp["name"] == "FINISH" and resp["value"] == "hmscan":
                self.end_scan(resp["ts"])
            self.event_queue.task_done()

    def wait_for_processed(self):
        # blocks until the queue is empty
        self.event_queue.join()

    def add_event(self, event):
        self.event_queue.put(event)

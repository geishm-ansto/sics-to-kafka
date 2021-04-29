
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

from datetime import datetime
from queue import Queue
from sicsclient.parsexml import parsesics, Component
from sicsclient.cmdbuilder import CommandBuilder
from sicsclient.kafkahelp import (KafkaProducer, timestamp_to_msecs,
                                  create_runstart_message, publish_message)
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
        self.transactions = 0
        self.ident = ident

        # create the thread that will rebuild the units
        self.thread = threading.Thread(target=self.process_events, daemon=True)
        self.thread.start()

    def zmq_failure(self):
        '''
        On a failure it will close the zmq socket it will clean up and pause
        '''
        if self.zsock:
            self.zpoller.unregister(self.zsock)
            self.zsock.setsockopt(zmq.LINGER, 0)
            self.zsock.close()
        self.zsock = None
        time.sleep(1)

    def open_connection(self):
        '''
        Create a new socket connection and register for polling
        '''
        # open the ZeroMQ message
        self.zsock = self.zcontext.socket(zmq.DEALER)
        self.zsock.setsockopt(zmq.IDENTITY, self.ident.encode('utf-8'))
        self.zsock.setsockopt(zmq.RCVTIMEO, self.recv_wait)
        self.zsock.connect(self.end_point.encode('utf-8'))
        self.zpoller.register(self.zsock, zmq.POLLIN)
        time.sleep(1)

        # test connection by checking status
        resp = self.cmd_request('SICS', 'status')
        if resp and resp['flag'].lower() == 'ok':
            logger.info('zmq.DEALER {} connection to {} OK'.format(
                self.ident, self.end_point))
            return True
        else:
            logger.warning('zmq.DEALER {} connection to {} FAILED'.format(
                self.ident, self.end_point))
            self.zmq_failure()
            return False

    def add_hdf_component(self, cmp, stream_source=None):
        '''
        Add each component to the command builderProcess each component where:
        . 'f142' type {txt,int,flt}, mutable
        . 'dataset' type {txt,int,flt}, not mutable

        The f142 writer does not support string streams so 'text' is captured as 
        a dataset.

        Map dtype to be compatible with the Nexus writer:
        . 'text': 'string'
        . 'int': 'int64'
        '''
        nxmap = {'text': 'string',
                 'int': 'int64'}
        try:
            dtype_ = nxmap[cmp.dtype]
        except KeyError:
            dtype_ = cmp.dtype
        if cmp.mutable:
            atts = [('units', cmp.units)] if cmp.units else []
            source = stream_source if stream_source else cmp.tag            
            self.cmd_builder.add_stream(
                # uses the simple stream 's142' rather than 'f142' which includes alarm
                # status information from nicos or epics
                cmp.tag, self.stream_topic, source, 's142', dtype_, cmp.value, atts)
        else:
            atts = [('units', cmp.units)] if cmp.units else []
            self.cmd_builder.add_dataset(cmp.tag, cmp.value, dtype_, atts)

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

        #dump xml for debugging
        ofile = './test/data/gumxml_latest.xml'
        with open(ofile, 'w') as f:
            f.write(resp['reply'])

        return clist

    def start_scan(self, scan_ts_secs):
        '''
        A histmem scan was STARTED:
        . Check if previous command was not completed and log message.
        . Get the kafkaxml structure and parse
        . Build the cmd structure
        . Notify the unit manager of the component list
        '''
        try:
            # if a start scan was initiated then raise an alert
            if self.cmd_builder:
                logger.info(
                    'Ignoring previous start scan that was not completed!')
            self.cmd_builder = None

            # get the component list form the xml description
            # and recover the key parameters
            # TODO the start and stop time should come from the xml parameters
            #       but the simulator has a completely wrong time that messes
            #       up the processing stop times in the writer.
            #       Use the start_scan timestap as the actual start of the recording
            # 
            clist = self.get_xml_parameters()
            starts = []  # find_nodes(clist, 'start_time')
            start_time = starts[0].value if starts else scan_ts_secs
            start_time_ms = timestamp_to_msecs(start_time)
            stops = []  # find_nodes(clist, 'stop_time')
            stop_time_ms = timestamp_to_msecs(
                stops[0].value) if stops else None
            fnames = find_nodes(clist, 'file_name')
            if fnames:
                basename = os.path.basename(fnames[0].value)
                ss = basename.split('.')
                file_name = ss[0] + '.nxs'
            else:
                logger.warning('Need file name to complete write request')
                return

            # load the default command string for the instrument
            self.cmd_builder = CommandBuilder(self.basefile)
            self.cmd_builder.set_param(
                filename=file_name, start_time_ms=start_time_ms,
                stop_time_ms=stop_time_ms, job_id=str(uuid.uuid4()))

            # specific parameters not recovered from xml dump
            # /entry/start_time, ../end_time  | stream : string
            # /entry/run_mode                 | stream : string  
            # /entry/experiment_identifier    | NA
            # /entry/time_stamp               | stream : int
            # /entry/program_name             | NA
            # /entry/program_revision         | NA
            # /entry/data ... folder /w links | build explicitly
            iso_start = datetime.fromtimestamp(start_time).isoformat(' ', 'seconds')
            addnl_cmps = [
                (Component("program_name", 'SICS-ESS', 'text', '', False, '', '', True), None),
                (Component("program_revision", 'NA', 'text', '', False, '', '', True), None),
                (Component("start_time", iso_start, 'text', '', False, '', '', True), None),
                (Component("run_mode", None, 'text', '', True, '', '', True), "entry/run_mode"),
                (Component("time_stamp", None, 'int', '', True, '', '', True), "entry/time_stamp")
            ]
            for cmp, src in addnl_cmps:
                self.add_hdf_component(cmp, src)

            # finally add the hdf nodes from the component list ad forward the component list
            # to the unit manager
            unit_values = {}
            for cmp in clist:
                self.add_hdf_component(cmp)
                unit_values[cmp.tag] = Parameter(cmp.value, cmp.units)
            self.unit_manager.set_unit_values(start_time, unit_values)

            # command builder may only require the stop time to be updated
        except Exception as e:
            logger.error(str(e))

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

        # for now only add the end time
        iso_end = datetime.fromtimestamp(finish_ts_secs).isoformat(' ', 'seconds')
        addnl_cmps = [
            (Component("end_time", iso_end, 'text', '', False, '', '', True), None)
        ]
        for cmp, src in addnl_cmps:
            self.add_hdf_component(cmp)

        # if the stop time was not specified then set it
        if not self.cmd_builder.get_stop_time():
            self.cmd_builder.set_param(
                stop_time_ms=timestamp_to_msecs(finish_ts_secs))

        # debugging code - remove
        ofile = './test/data/temp.json'
        self.cmd_builder.save(ofile)

        # send to the writer command topic
        self.send_write_cmd(timestamp_to_msecs(finish_ts_secs))

        # For now just issue a write command and forget, if we need a status
        # then the command builder or the job id may need to managed until
        # the write is confirmed. Just delete until more is needed.
        logger.info('Issued write request to the nexus writer topic')
        self.cmd_builder = None

    def send_write_cmd(self, timestamp_ms):
        msg = create_runstart_message(self.cmd_builder.get_command())
        publish_message(self.kafka_producer, self.writer_topic, timestamp_ms, msg)

    def cmd_request(self, cmd, text):

        if not self.zsock and not self.open_connection():
            return {}

        self.transactions += 1
        request = {
            'trans': self.transactions,
            'cmd': cmd,
            'text': text
        }
        msg = json.dumps(request).encode('utf-8')
        try:
            self.zsock.send(msg)
            while True:
                message = self.zsock.recv().decode('utf-8')
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
            self.zmq_failure()
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
        self.zsock = None
        self.zcontext = zmq.Context()
        self.zpoller = zmq.Poller()
        self.open_connection()

        while True:
            # get the message from the queue
            resp = self.event_queue.get()
            if resp is None:
                continue

            # do something useful
            logger.info('{:.3f} {} {}'.format(
                resp["ts"], resp["name"], resp["value"]))
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

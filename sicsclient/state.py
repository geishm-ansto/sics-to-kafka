
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
from queue import Queue, Empty
from sicsclient.parsexml import parsesics, Component
from sicsclient.cmdbuilder import CommandBuilder
from sicsclient.kafkahelp import (KafkaProducer, timestamp_to_msecs,
                                  create_runstart_message, create_runend_message,
                                  publish_message)
from sicsclient.helpers import setup_module_logger

logger = setup_module_logger("sk.state")


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

    def __init__(self, sics, port, basefile, writer_status, kafka_broker='localhost:9092', recv_wait=1000, ident='sics-kafka',
                 stream_topic='sics-stream', writer_topic='TEST_writerCommand', timeout_secs=30):

        self._basefile = basefile
        self._writer_status = writer_status
        self._stream_topic = stream_topic
        self._writer_topic = writer_topic
        self._kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)
        self._event_queue = Queue()
        self._recv_wait = recv_wait
        self._end_point = 'tcp://{}:{}'.format(sics, port)
        self._transactions = 0
        self._timeout_secs = timeout_secs
        self._ident = ident
        self._job_id = None

        # create the thread that will rebuild the units
        self._zcontext = self._zpoller = self._zsock = None
        thread = threading.Thread(target=self.process_events, daemon=True)
        thread.start()

    def on_zmq_failure(self):
        '''
        On a failure it will close the zmq socket it will clean up and pause
        '''
        if self._zcontext:
            self._zpoller.unregister(self._zsock)
            self._zcontext.destroy(linger=0)
            self._zcontext = self._zpoller = self._zsock = None
        time.sleep(1)

    def open_connection(self):
        '''
        Create a new socket connection and register for polling
        '''
        # open the ZeroMQ message
        self._zcontext = zmq.Context()
        self._zsock = self._zcontext.socket(zmq.DEALER)
        self._zsock.setsockopt(zmq.IDENTITY, self._ident.encode('utf-8'))
        self._zsock.setsockopt(zmq.RCVTIMEO, self._recv_wait)
        self._zsock.connect(self._end_point.encode('utf-8'))
        self._zpoller = zmq.Poller()
        self._zpoller.register(self._zsock, zmq.POLLIN)
        time.sleep(1)

        # test connection by checking status
        resp = self.cmd_request('SICS', 'status')
        if resp and resp['flag'].lower() == 'ok':
            logger.info('zmq.DEALER {} connection to {} OK'.format(
                self._ident, self._end_point))
            return True
        else:
            logger.warning('zmq.DEALER {} connection to {} FAILED'.format(
                self._ident, self._end_point))
            self.on_zmq_failure()
            return False

    def add_hdf_component(self, cmd_builder, cmp, stream_source=None):
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
            # uses the simple stream 's142' rather than 'f142' which includes alarm
            # status information from nicos or epics
            cmd_builder.add_stream(
                cmp.tag, self._stream_topic, source, 's142', dtype_, cmp.value, atts)
        else:
            atts = [('units', cmp.units)] if cmp.units else []
            cmd_builder.add_dataset(cmp.tag, cmp.value, dtype_, atts)

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
            logger.error('Unable to recover xml parameters from SICS')
            clist = []

        # TODO remove dump xml for development
        ofile = './test/data/gumxml_latest.xml'
        with open(ofile, 'w') as f:
            f.write(resp['reply'])

        return clist

    def start_scan(self, scan_ts_secs):
        '''
        A histmem scan was STARTED:
        . Get the kafkaxml structure and parse
        . Build the cmd structure
        . Cancel any active job 
        . Send the command (with stop set to 0)
        '''
        try:
            # get the component list form the xml description
            # and recover the key parameters
            clist = self.get_xml_parameters()
            fnames = find_nodes(clist, 'file_name')
            if fnames:
                basename = os.path.basename(fnames[0].value)
                ss = basename.split('.')
                filename = ss[0] + '.nxs'
            else:
                logger.error('Need file name to complete write request')
                return

            # load the default command string for the instrument
            start_time = scan_ts_secs
            start_time_ms = timestamp_to_msecs(start_time)
            self._job_id = str(uuid.uuid4())
            cmd_builder = CommandBuilder(self._basefile)
            cmd_builder.set_param(
                filename=filename, start_time_ms=start_time_ms,
                stop_time_ms=0, job_id=self._job_id)

            # add the mutable nodes from the xml description
            for cmp in clist:
                self.add_hdf_component(cmd_builder, cmp)

            # specific parameters not recovered from xml dump
            # /entry/start_time, ../end_time  | stream : string
            # /entry/run_mode                 | stream : string
            # /entry/experiment_identifier    | NA
            # /entry/time_stamp               | stream : int
            # /entry/program_name             | NA
            # /entry/program_revision         | NA
            # /entry/data ... folder /w links | build explicitly
            iso_start = datetime.fromtimestamp(
                start_time).isoformat(' ', 'seconds')
            addnl_cmps = [
                (Component("program_name", 'SICS-ESS',
                 'text', '', False, '', '', True), None),
                (Component("program_revision", 'NA',
                 'text', '', False, '', '', True), None),
                (Component("start_time", iso_start,
                 'text', '', False, '', '', True), None),
                (Component("run_mode", None, 'text', '',
                 True, '', '', True), "entry/run_mode"),
                (Component("time_stamp", None, 'int', '',
                 True, '', '', True), "entry/time_stamp")
            ]
            for cmp, src in addnl_cmps:
                self.add_hdf_component(cmd_builder, cmp, src)

            # TODO debugging code - remove
            ofile = './test/data/temp.json'
            cmd_builder.save(ofile)

            # finally get the command to be send to the writer
            write_command = cmd_builder.get_command()

            # before sending the write command cancel any active job with the
            # writer because it only supports a sequential write process
            if not self.cancel_active_job():
                logger.error(
                    'Timed out cancelling the active nexus writer job')

            # send to the writer command topic
            # For now just issue a write command and forget, if we need a status
            # then the command builder or the job id may need to managed until
            # the write is confirmed. Just delete until more is needed.
            self.send_start_cmd(timestamp_to_msecs(
                scan_ts_secs), write_command)
            logger.info(f'Start nexus writer, file: {filename} job id: {self._job_id}')

        except Exception as e:
            logger.error(str(e))

    def end_scan(self, finish_ts_secs):
        '''
        Stop a currently executing job id by sending a stop message and
        then clear the job id.
        '''
        if self._job_id:
            self.send_stop_cmd(timestamp_to_msecs(
                finish_ts_secs), self._job_id)
            logger.info('Stop nexus writer, job id: {}'.format(self._job_id))
            self._job_id = None
        else:
            logger.warning('Missing job id, may have timed out, do nothing!')

    def handle_idle_event(self):
        '''
        If there is an active job that has not been closed then issue the stop event.
        Note that this call is in the same thread as starting and stopping scans so it 
        does not to lock the job_id.
        '''
        if self._job_id:
            self.send_stop_cmd(1, self._job_id)
            logger.info('Stop nexus writer, job id: {}'.format(self._job_id))
            self._job_id = None

    def cancel_active_job(self):
        active_job = self._writer_status.get_active_job()
        if active_job:
            self.send_stop_cmd(1, active_job)
            logger.info('Aborting nexus writer, job id: {}'.format(active_job))
        return self._writer_status.wait_for_idle(self._timeout_secs)

    def send_start_cmd(self, timestamp_ms, params):
        msg = create_runstart_message(params)
        publish_message(self._kafka_producer,
                        self._writer_topic, timestamp_ms, msg)

    def send_stop_cmd(self, timestamp_ms, job_id):
        msg = create_runend_message(timestamp_ms, job_id)
        publish_message(self._kafka_producer,
                        self._writer_topic, timestamp_ms, msg)

    def cmd_request(self, cmd, text):

        if not self._zsock and not self.open_connection():
            return {}

        self._transactions += 1
        request = {
            'trans': self._transactions,
            'cmd': cmd,
            'text': text
        }
        msg = json.dumps(request).encode('utf-8')
        response = {}
        try:
            self._zsock.send(msg, zmq.NOBLOCK)
            while True:
                resp = self._zpoller.poll(self._timeout_secs * 1000)
                if resp:
                    message = self._zsock.recv().decode('utf-8')
                    if message.startswith("{"):
                        response = json.loads(message)
                    else:
                        response = {'reply': "Nothing"}
                    if 'final' in response and response['final']:
                        return response
                else:
                    raise ValueError('dealer timeout error')

        except Exception as exc:
            # There is a significant risk of a dead lock with the ZMQ client server model
            # so after printing the message re-establish the connection
            logger.error('ZeroMQ: {}'.format(str(exc)))
            self.on_zmq_failure()
        return response

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
        self.open_connection()

        while True:
            try:
                # get the message from the queue - if it times out after N sec
                # check the sics status
                resp = self._event_queue.get(timeout=self._timeout_secs)
                if resp is None:
                    continue

                # do something useful
                logger.info('{:.3f} {} {}'.format(
                    resp["ts"], resp["name"], resp["value"]))
                if resp["type"] == "State" and resp["name"] == "STARTED" and resp["value"] == "hmscan":
                    self.start_scan(resp["ts"])
                elif resp["type"] == "State" and resp["name"] == "FINISH" and resp["value"] == "hmscan":
                    self.end_scan(resp["ts"])
                elif resp["type"] == "Status" and resp["name"] == "Status" and \
                        ('UNDEFINED' in resp['value'] or "Eager to execute" in resp["value"]):
                    self.handle_idle_event()
                self._event_queue.task_done()
            except Empty:
                self.sics_monitor()

    def wait_for_processed(self):
        # blocks until the queue is empty
        self._event_queue.join()

    def add_event(self, event):
        self._event_queue.put(event)

    def sics_monitor(self):
        resp = self.cmd_request('SICS', 'status')
        start_time = time.time()
        evt = {'ts': start_time,
               'type': 'Status',
               'name': 'Status'}
        if not resp:
            evt['value'] = 'UNDEFINED: SICS router timeout'
        else:
            evt['value'] = resp['reply']
        self.add_event(evt)

    @property
    def zmq_socket(self):
        return self._zsock

    @property
    def kafka_producer(self):
        return self._kafka_producer

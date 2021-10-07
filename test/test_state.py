#!/usr/bin/python

"""
Tests the StateProcessor class which is responsible for:
- connects to the SICS server 
- reconnects to the SICS server if a connection is interrupted
- 
- managing the state transition idle -> startscan -> endscan 
- collect the XML description
- build the command file 

The test cases attempt to mock all of the dependent interfaces.
"""
import os
import threading
import time
import unittest
import zmq
import json
import copy

from unittest.mock import patch
from sicsclient.state import StateProcessor, find_nodes
from sicsclient.parsexml import Component
from sicsclient.kafkahelp import (timestamp_to_msecs, extract_runstart_data,
                                  extract_runstop_data, create_runstart_message,
                                  create_runend_message)

c_base_file = './config/pln_base.json'
c_ofile = './some_scan.hdf'
c_xml_ifile = './test/data/gumtreexml.xml'
c_sics_url = "localhost"
c_sics_port = 5554

component_list = [
    # 'tag', 'value', 'dtype', 'klass', 'mutable', 'nxalias', 'units', 'nxsave'
    Component('experiment/file_name', c_ofile,
              'text', '', False, '', '', True),
    Component('experiment/start_time', 1585519280,
              'int', '', False, '', '', True),
    Component('sample/temperature', 85.3, 'float', '', True, '', 'DegC', True),
    Component('sample/name', 'XXXX', 'text', '', False, '', '', True),
]


class ZmqRouter:
    '''
    Implemented as a locked context manager to ensure the objects. 
    '''
    zmq_lock = threading.Lock()

    def __init__(self, port):
        '''
        Create a new socket connection and register for polling
        '''
        self.port = port
        self.context = None

    def __enter__(self):
        self.zmq_lock.acquire()
        self._open_connection()
        return self

    def __exit__(self, type, value, traceback):
        self._close_connection()
        time.sleep(1)   # wait for the connection to close
        self.zmq_lock.release()

    def _open_connection(self):
        self.context = zmq.Context()
        self.zsock = self.context.socket(zmq.ROUTER)
        address = "tcp://*:{:d}".format(self.port)
        self.zsock.bind(address)
        self.poll = zmq.Poller()
        self.poll.register(self.zsock, zmq.POLLIN)

        self.send_reply = False
        self.thread = threading.Thread(target=self.handle_input)
        self.thread.start()

    def _close_connection(self):
        if self.context:
            self.poll.unregister(self.zsock)
            self.context.destroy(linger=0)
            self.context = self.poll = self.zsock = None

    def start(self):
        self.send_reply = True
        time.sleep(1)

    def stop(self):
        self.send_reply = False
        time.sleep(1)

    def handle_input(self):
        while self.context:
            try:
                #  Wait for next request from client
                if self.poll.poll(500):
                    id = self.zsock.recv()
                    message = self.zsock.recv()
                    if self.send_reply:
                        resp = self.response(message)
                        if resp:
                            msg = json.dumps(resp).encode('utf-8')
                            self.zsock.send(id, zmq.SNDMORE)
                            self.zsock.send(msg)
            except zmq.error.ZMQError:
                time.sleep(1)

    def response(message):
        # over ride behaviour in mock
        return {'flag': 'ok', 'final': True, 'reply': ''}


def my_get_xml(self):
    return component_list


publish_message_mock = unittest.mock.Mock()

writer_status = unittest.mock.Mock()

c_status_req = b'{"trans": 1, "cmd": "SICS", "text": "status"}'


class TestStateProcessor(unittest.TestCase):
    '''
    The tests to be considered:
    - recovering the xml file from SICS
    - parsing the xml file from SICS
    - build the command argument 
    '''

    def test_confirm_startmsg(self):
        cmds = {
            "job_id": "some_key",
            "filename": "test_file.nxs",
            "start_time": 567890,
            "stop_time": 578214,
            "run_name": "test_run",
            "nexus_structure": "{}",
            "service_id": "filewriter1",
            "instrument_name": "LOKI",
            "broker": "localhost:9092",
            "metadata": "{3:1}",
            "detector_spectrum_map": None,
            "control_topic": "some_topic_name",
        }
        msg = create_runstart_message(cmds)
        data = extract_runstart_data(msg)
        for k, v in cmds.items():
            dv = getattr(data, k)
            self.assertEqual(dv, v)

    def test_confirm_stopmsg(self):
        cmds = {
            "job_id": "some_key",
            "stop_time": 578214,
        }
        msg = create_runend_message(**cmds)
        data = extract_runstop_data(msg)
        for k, v in cmds.items():
            dv = getattr(data, k)
            self.assertEqual(dv, v)

    def confirm_stop_sent(self, _kprod, job_id):
        _kprod.send.assert_called()
        args, kwargs = _kprod.send.call_args
        self.assertEqual(args[0], 'TEST_writerCommand')
        resp = extract_runstop_data(kwargs['value'])
        self.assertGreater(resp.stop_time, 0)
        self.assertEqual(resp.job_id, job_id)

    @patch('sicsclient.state.logger')
    @patch('sicsclient.state.zmq')
    @patch('sicsclient.state.KafkaProducer')
    @patch('sicsclient.state.StateProcessor.cmd_request')
    def test_zmq_connection(self, _cmd, _producer, _zmq, _logger):
        '''
        Confirms that it establishes the connection to the correct url and port number
        '''
        _cmd.return_value = {'flag': 'ok'}
        stp = StateProcessor(
            c_sics_url, c_sics_port, c_base_file, writer_status=writer_status, ident='test_zmq_connection')

        # confirm that only call connect once because it was ok
        time.sleep(5)
        zsock = stp.zmq_socket
        zsock.connect.assert_called_once_with(
            'tcp://{}:{}'.format(c_sics_url, c_sics_port).encode('utf-8'))

    @patch('sicsclient.state.logger')
    @patch('test_state.ZmqRouter.response')
    @patch('sicsclient.state.KafkaProducer')
    def test_zmq_reconnect(self, _producer, _response, _logger):
        '''
        Confirms that on the loss of connection it logs an error and then 
        reconnects when the zerver is re-estabslished.
        '''
        with ZmqRouter(c_sics_port) as router:
            _response.return_value = {'flag': 'ok', 'final': True}
            router.start()
            stp = StateProcessor(
                c_sics_url, c_sics_port, c_base_file, writer_status=writer_status, ident='test_zmq_reconnect')

            # everything should be ok - no logged errors
            time.sleep(5)
            _response.assert_called_once()
            _response.assert_called_once_with(c_status_req)
            self.assertFalse(_logger.error.called)

            # confirm that the cmd_request sends the command to the router
            _response.reset_mock()
            stp.cmd_request('ABC', 'defgh')
            time.sleep(1)
            _response.assert_called_once()
            args, _ = _response.call_args
            msg = json.loads(args[0].decode('utf-8'))
            self.assertEqual(msg['cmd'], 'ABC')
            self.assertEqual(msg['text'], 'defgh')

            # delete the router and invoke a cmd request to generate an error
            router.stop()
            time.sleep(5)
            stp.cmd_request('SICS', 'status')
            self.assertTrue(_logger.error.called)
            _response.reset_mock()

            # restore the connection and confirm that connection is established again
            router.start()
            time.sleep(1)
            stp.cmd_request('SICS', 'status')
            _response.assert_called()

    @patch('test_state.ZmqRouter.response')
    @patch('sicsclient.state.KafkaProducer')
    def test_getxml_request(self, _producer, _response):
        '''
        Confirm that the correct command is sent to the ZMQ connection by mocking
        the zmq socket.
        '''
        with ZmqRouter(c_sics_port) as router:
            _response.return_value = {'flag': 'ok', 'final': True, 'reply': ''}
            router.start()
            stp = StateProcessor(
                c_sics_url, c_sics_port, c_base_file, writer_status=writer_status, ident='test_getxml_request')
            time.sleep(5)
            _response.reset_mock()
            stp.get_xml_parameters()
            _response.assert_called_once()
            args, _ = _response.call_args
            msg = json.loads(args[0].decode('utf-8'))
            self.assertEqual(msg['cmd'], 'SICS')
            self.assertEqual(msg['text'], 'getgumtreexml /')
            router.stop()

    @patch('test_state.ZmqRouter.response')
    @patch('sicsclient.state.KafkaProducer')
    def test_recover_xml_params(self, _producer, _response):
        '''
        Recover the xml data by directly issuing the request to the SICS server
        and confirm that it is non-empty and that 'file_name' is included.
        This test requires the ssh tunnel to be setup prior to the test.
        '''
        with ZmqRouter(c_sics_port) as router:
            _response.return_value = {'flag': 'ok', 'final': True, 'reply': ''}
            router.start()
            stp = StateProcessor(
                c_sics_url, c_sics_port, c_base_file, writer_status=writer_status)
            time.sleep(2)  # wait for status ok

            # load the test xml data
            with open(c_xml_ifile, 'r') as f:
                xmlstr = f.read()
            _response.reset_mock()
            _response.return_value = {
                'flag': 'ok', 'final': True, 'reply': xmlstr}

            # now call the method and confirm that a few nodes can be found
            clist = stp.get_xml_parameters()
            self.assertTrue(clist)
            nodes = find_nodes(clist, 'file_name')
            self.assertTrue(nodes)

    @patch('test_state.StateProcessor.get_xml_parameters', my_get_xml)
    @patch('sicsclient.state.logger')
    @patch('sicsclient.state.zmq')
    @patch('sicsclient.state.KafkaProducer')
    @patch('sicsclient.state.StateProcessor.cmd_request')
    def test_capture_scan(self, _cmd, _kafka, _zmq, _logger):
        '''
        Mock the following objects and calls:
        . get_xml_parameters

        Confirms that the
        send the start command by adding it to the list of events to be processed and
        wait for the processing to complete
        patch in a mock to replace the command builder for this test - mock the object
        command builder imported into module sicstate
        '''
        _cmd.return_value = {'flag': 'ok', 'final': True, 'reply': ''}
        stp = StateProcessor(
            c_sics_url, c_sics_port, c_base_file, writer_status=writer_status, ident='test_zmq_connection')

        # issue a start command using the current time
        writer_status.get_active_job.return_value = "123456"
        writer_status.wait_for_idle.return_value = True
        start_time = time.time()
        evt = {'ts': start_time,
               'type': 'State',
               'name': 'STARTED',
               'value': 'hmscan'}
        stp.add_event(evt)
        time.sleep(10)

        # confirm that it checked the writer status, sent a stop command and
        # that it waited for the the write to close before continuing
        writer_status.get_active_job.assert_called_once()
        writer_status.wait_for_idle.assert_called_once()

        # confirm cmd was sent to the kafka writer
        _kprod = stp.kafka_producer
        args, kwargs = _kprod.send.call_args
        self.assertEqual(args[0], 'TEST_writerCommand')
        self.assertEqual(kwargs['timestamp_ms'],
                         timestamp_to_msecs(start_time))
        resp = extract_runstart_data(kwargs['value'])
        self.assertEqual(resp.start_time, timestamp_to_msecs(start_time))
        self.assertEqual(resp.stop_time, 0)
        basename = os.path.basename(c_ofile)
        ss = basename.split('.')
        file_name = ss[0] + '.nxs'
        self.assertEqual(resp.filename, file_name)
        job_id = resp.job_id

        # now send a stop command
        _kprod.reset_mock()
        evt = {'ts': start_time + 10,
               'type': 'State',
               'name': 'FINISH',
               'value': 'hmscan'}
        stp.add_event(evt)
        time.sleep(1)
        args, kwargs = _kprod.send.call_args
        self.assertEqual(args[0], 'TEST_writerCommand')
        self.assertEqual(kwargs['timestamp_ms'],
                         timestamp_to_msecs(start_time + 10))
        resp = extract_runstop_data(kwargs['value'])
        self.assertEqual(resp.stop_time,
                         timestamp_to_msecs(start_time + 10))
        self.assertEqual(resp.job_id, job_id)

    @patch('test_state.StateProcessor.get_xml_parameters', my_get_xml)
    @patch('sicsclient.state.logger')
    @patch('test_state.ZmqRouter.response')
    @patch('sicsclient.state.KafkaProducer')
    def test_hanging_scan(self, _kafka, _response, _logger):
        '''
        If SICS drops the scan without issuing a 'FINISH hmscan' the
        writer process will continue. If there is no status for a period
        of time or the status is 'Eager to execute' while there is an active 
        job then the job will be cancelled.
        '''
        def get_job_id(_kprod):
            args, kwargs = _kprod.send.call_args
            resp = extract_runstart_data(kwargs['value'])
            return copy.copy(resp.job_id)

        with ZmqRouter(c_sics_port) as router:
            _response.return_value = {
                'flag': 'ok', 'final': True, 'reply': 'Eager to execute'}
            router.start()
            stp = StateProcessor(
                c_sics_url, c_sics_port, c_base_file, writer_status=writer_status, ident='test_zmq_connection', timeout_secs=15)
            time.sleep(2)

            # mock the return values for active job so it starts the scan
            writer_status.get_active_job.return_value = ""
            writer_status.wait_for_idle.return_value = True
            _kprod = stp.kafka_producer
            _kprod.reset_mock()
            start_time = time.time()
            start_ev = {'ts': start_time,
                        'type': 'State',
                        'name': 'STARTED',
                        'value': 'hmscan'}
            stp.add_event(start_ev)
            time.sleep(2)

            _kprod.send.assert_called_once()
            job_id = get_job_id(_kprod)

            # issue a idle status before active job closed and confirm a stop
            # request is sent
            idle_ev = {'ts': start_time,
                       'type': 'Status',
                       'name': 'Status',
                       'value': 'Eager to execute'}
            stp.add_event(idle_ev)
            time.sleep(10)
            self.confirm_stop_sent(_kprod, job_id)

            # suspend the zmq connection and confirm that a stop request is issued to
            # the nexus writer
            _kprod.reset_mock()
            stp.add_event(start_ev)
            time.sleep(2)
            router.stop()
            _kprod.send.assert_called_once()
            job_id = get_job_id(_kprod)
            _kprod.reset_mock()
            time.sleep(30)
            self.confirm_stop_sent(_kprod, job_id)
            time.sleep(1)


if __name__ == '__main__':
    unittest.main()

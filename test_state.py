#!/usr/bin/python

"""
Tests the StateProcessor class which is responsible for:
- managing the state transition idle -> startscan -> endscan 
- collect the XML description
- build the command file 
The tests do not mock do actual SICS service so an SSH tunnel needs to be setup by:
$ ssh -L 5555:ics1-pelican-test.nbi.ansto.gov.au:5555 geishm@ics1-pelican-test.nbi.ansto.gov.au
The server needs to be active by
$ sudo systemctl [status/start] sics_server
"""
import os
import zmq
import time
import json
import unittest
import threading

from unittest.mock import patch
from sicsstate import StateProcessor, find_nodes
from parsexml import Component
from kafkahelp import timestamp_to_msecs

base_file = './config/pln_base.json'

component_list = [
    # 'tag', 'value', 'dtype', 'klass', 'mutable', 'nxalias', 'units', 'nxsave'
    Component('experiment/file_name', './some_scan.hdf',
              'text', '', False, '', '', True),
    Component('experiment/start_time', 1585519280,
              'int', '', False, '', '', True),
    Component('sample/temperature', 85.3, 'float', '', True, '', 'DegC', True),
    Component('sample/name', 'XXXX', 'text', '', False, '', '', True),
]

# Some mocking objects
def my_get_xml(self):
    return component_list

send_write_mock = unittest.mock.Mock()

class TestStateProcessor(unittest.TestCase):
    '''
    The tests to be considered:
    - recovering the xml file from SICS
    - parsing the xml file from SICS
    - build the command argument 
    '''

    @classmethod
    def setUpClass(self):
        # create the StateProcessor object for the tests
        unm = unittest.mock.Mock()
        TestStateProcessor.stp = StateProcessor(
            'localhost', 5555, base_file, unit_manager=unm)

    def test_recoverxml(self):
        '''
        Recover the xml data by directly issuing the request to the SICS server
        and confirm that it is non-empty and that 'file_name' is included.
        '''
        clist = TestStateProcessor.stp.get_xml_parameters()
        self.assertTrue(clist)
        nodes = find_nodes(clist, 'file_name')
        self.assertTrue(nodes)

    @patch('test_state.StateProcessor.get_xml_parameters', my_get_xml)
    def test_start_scan(self):
        '''
        Mock the following objects and calls:
        . UnitManager (setup for class)
        . get_xml_parameters

        Confirms that the 
        send the start command by adding it to the list of events to be processed and
        wait for the processing to complete
        patch in a mock to replace the command builder for this test - mock the object
        command builder imported into module sicstate
        '''
        stp = TestStateProcessor.stp
        start_time = time.time()
        stp.start_scan(start_time)

        self.assertTrue(stp.cmd_builder != None)
        cmb = stp.cmd_builder
        start_cmp = find_nodes(component_list, 'start_time')[0]
        self.assertEqual(timestamp_to_msecs(start_cmp.value), cmb.get_start_time())

        # look for the components relative to the root node
        rnode = cmb.get_root()
        for c in component_list:
            cpath = c.tag.split('/')
            cpath.insert(0, '.')
            pnode = cmb._find_node(rnode, cpath, create=False)
            self.assertTrue(pnode)
            if c.mutable:
                self.assertEqual(pnode['type'], 'group')
                self.assertEqual(pnode['name'], cpath[-1])
            else:
                self.assertEqual(pnode['type'], 'dataset')
                self.assertEqual(pnode['name'], cpath[-1])
                self.assertEqual(pnode['values'], c.value)

        # confirm that the unit manager receives the unit values
        cargs = stp.unit_manager.set_unit_values.call_args[0]
        self.assertEqual(cargs[0], start_cmp.value)
        arg_values = cargs[1]
        for c in component_list:
            self.assertEqual(c.value, arg_values[c.tag].value)

    @patch('test_state.StateProcessor.get_xml_parameters', my_get_xml)
    @patch('test_state.StateProcessor.send_write_cmd', send_write_mock)
    def test_finish_scan(self):
        '''
        Mock the following objects and calls:
        . UnitManager (setup for class)
        . get_xml_parameters
        . KafkaProducer

        Initiates a start_scan and then invokes the finish.
        Confirms that the 
        send the start command by adding it to the list of events to be processed and
        wait for the processing to complete
        patch in a mock to replace the command builder for this test - mock the object
        command builder imported into module sicstate
        '''
        stp = TestStateProcessor.stp
        start_time = time.time()
        stp.start_scan(start_time)

        stop_time = int(start_time) + 60
        stp.end_scan(stop_time)

        # confirm cmd was sent to the kafka writer
        cargs = stp.send_write_cmd.call_args[0]
        self.assertEqual(cargs[0], timestamp_to_msecs(stop_time))
        cmd = json.loads(cargs[1])
        self.assertEqual(cmd['cmd'], 'FileWriter_new')


if __name__ == '__main__':
    unittest.main()

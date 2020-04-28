#!/usr/bin/python

"""
Tests the StateProcessor class which is responsible for:
- managing the state transition idle -> startscan -> endscan 
- collect the XML description
- build the command file 
"""

import os
import zmq
import time
import json
import unittest
import threading

from unittest.mock import patch
from sicsstate import StateProcessor

base_file = './config/pln_base.json'

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
        TestStateProcessor.stp = StateProcessor('localhost', 5555, base_file)

    def _test_raw_recoverxml(self):
        # recover the xml data by directly issuing the request
        xmlfile = './data/gumtreexml.xml'
        resp = TestStateProcessor.stp.cmd_request('SICS', 'getgumtreexml /')
        if resp and resp['flag'].lower() == 'ok':
            if not os.path.isfile(xmlfile):
                with open(xmlfile, 'w') as f:
                    f.write(resp["reply"])
        else:
            self.assertFalse('Unable to recover the raw xml data')

    def _test_recoverxml(self):
        # send the start command by adding it to the list of events to be processed and
        # wait for the processing to complete
        # patch in a mock to replace the command builder for this test - mock the object
        # command builder imported into module sicstate
        start_json = b'{ "type": "State", "name": "STARTED", "value": "hmscan", "seq": 1, "ts": 1585519280.073885 }'
        msg = json.loads(start_json)
        with patch('sicsstate.CommandBuilder') as MockBuilder:
            instance = MockBuilder.return_value

            TestStateProcessor.stp.add_event(msg)
            TestStateProcessor.stp.wait_for_update()

            instance.base_command.assert_called_once_with(base_file, msg['ts'])

    def test_parsexml(self):
        # mock the 
        pass

    def _test_buildcmd(self):
        pass


if __name__ == '__main__':
    unittest.main()

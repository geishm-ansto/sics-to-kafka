#!/usr/bin/python

"""
Tests the CommandBuilder class which is responsible for generating
the command string for the kafka-to-nexus writer
"""

import os
import zmq
import time
import json
import unittest

from parsexml import parsesics

testfile = './data/gumtreexml.xml'


class TestXMLParser(unittest.TestCase):

    def test_xmltodict(self):

        expected = [
            'control/T1S1',
            'control/T1S2',
            'control/T1S3',
            'control/T1S4',
            'control/T1SP1',
            'control/T1SP2',
            #'experiment/gumtree_status',
            #'experiment/gumtree_time_estimate'
        ]
        with open(testfile, 'r') as fd:
            clist = parsesics(fd.read())

        tosave = [cl for cl in clist if cl.nxsave]
        for cl, tag in zip(tosave, expected):
            print('{} -> {}'.format(cl.tag, cl.nxalias))
            self.assertEqual(cl.tag, tag)

if __name__ == '__main__':
    unittest.main()

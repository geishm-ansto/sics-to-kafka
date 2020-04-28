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

from parsexml import parse

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
            'experiment/gumtree_status',
            'experiment/gumtree_time_estimate'
        ]
        clist = parse(testfile)
        for cl, tag in zip(clist, expected):
            print('{} -> {}'.format(cl.tag, cl.nxalias))
            self.assertEqual(cl.tag, tag)

if __name__ == '__main__':
    unittest.main()

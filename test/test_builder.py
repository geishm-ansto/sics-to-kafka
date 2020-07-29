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

from datetime import datetime
from sicsclient.cmdbuilder import CommandBuilder
from sicsclient.helpers import unix_time_milliseconds

base_file = './config/pln_base.json'

class TestBuilder(unittest.TestCase):
  '''
  Test sequence:
  - check default command is loaded 
  - build equivalent to loaded file
  - add elements
  '''

  def test_loadedfile(self):
    builder = CommandBuilder(base_file)
    cmd = builder.get_command()
    self.assertEqual(cmd["job_id"], "JOBID")
    self.assertEqual(cmd["start_time"], "STARTTIME")
    self.assertEqual(cmd["stop_time"], "STOPTIME")
    node = builder.get_root()
    self.assertTrue(node != None)
    self.assertEqual(node["name"], "entry1")
  
  def test_build(self):
    # values
    ofile = './test/data/temp.json'    
    start = int(unix_time_milliseconds(datetime.utcnow()))
    stop = start + 1000 * 600
    root = "root"
    broker="somewhere:9092"

    builder = CommandBuilder()
    builder.base_command(ofile, start, stop, root=root, broker=broker)

    job_id = builder.get_job_id()

    builder.save(ofile)

    builder = CommandBuilder(ofile)
    cmd = builder.get_command()
    self.assertEqual(cmd["job_id"], job_id)
    self.assertEqual(cmd["start_time"], start)
    self.assertEqual(cmd["stop_time"], stop)
    node = builder.get_root()
    self.assertTrue(node != None)
    self.assertEqual(node["name"], root)    

  def test_elements(self):
    # path names are relative root
    datasets = [
      ('instrument/chopper/freqs', [10.1, 10.3], 'float', [('units','Hz')]),
      ('frequency', 10.1, 'float', [('units','Hz')]),
      ('start_time', '2018-11-12 10:45:06', 'string', []),
      ('nested_list', [[1,2],[3,4]], 'float', [('units','Hz')])
    ]

    # name, topic, source, writer, dtype, attributes
    streams = [
      ('sample/some_temp', 'sics_stream', 'sample/some_temp', 'f142', 'float', 12.3, [('units','Deg')]),
      ('sample/fixed_temp', 'sics_stream', 'sample/fixed_temp', 'sval', 'float', 0.0, [('units','Deg C')])
    ]

    builder = CommandBuilder(base_file, name_stream=True)
    for dt in datasets:
      builder.add_dataset(*dt)
    for st in streams:
      builder.add_stream(*st)

    # save to temp file and reload
    ofile = './test/data/temp.json'
    builder.save(ofile)
    builder = CommandBuilder(ofile)

    root = builder.get_root()
    for dt in datasets:
      npath = ("./" + dt[0]).split("/")
      node = builder._find_node(root, npath)
      self.assertTrue(node != None)
      self.assertEqual(node["name"], npath[-1])
      self.assertEqual(node["values"], dt[1])
    for st in streams:
      npath = ("./" + st[0]).split("/")
      node = builder._find_node(root, npath)
      self.assertTrue(node != None)
      self.assertEqual(node["name"], npath[-1])


if __name__ == '__main__':
    unittest.main()
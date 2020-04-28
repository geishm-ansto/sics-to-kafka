#!/usr/bin/python

"""
Tests that the units manager extracts the units from the SICS platform. The tests requires
the SICS to be executing on 'ics1-pelican-test.nbi.ansto.gov.au' with remote access or an 
ssh tunnel has been setup with:
$ ssh -L 5555:ics1-pelican-test.nbi.ansto.gov.au:5555 geishm@ics1-pelican-test.nbi.ansto.gov.au
"""

import zmq
import time
import json
import threading
import unittest
from sicsunits import UnitManager

class TestUnits(unittest.TestCase):

    units = {
        '/monitor/bm1_counts': '',
        '/monitor/bm1_event_rate': 'count/sec',
        '/instrument/aperture/sh1': 'mm',
        '/instrument/detector/tofw': 'microseconds',
        '/instrument/detector/total_counts': 'count',
        '/sample/bsr': 'degrees'
    }

    def test_recover(self):

        mgr = UnitManager('localhost', 5555)
        
        for tag, unit in self.units.items():
            lbl = mgr.recover_unit(tag)
            self.assertEqual(unit, lbl)

        # now clear the units add the items and wait for the messages to be updated
        mgr.clear_units()
        for tag in self.units.keys():
            mgr.add_unit(tag)
        mgr.wait_for_update()
        for tag, unit in self.units.items():
            lbl = mgr.get_unit(tag)
            self.assertEqual(unit, lbl)

    def collect_params(self, myunits, index):

        print('Starting thread {}'.format(index))
        #time.sleep(1)
        mgr = UnitManager('localhost', 5555, ident=str(index))
        for tag in myunits.keys():
            mgr.add_unit(tag)
        mgr.wait_for_update()
        for tag, unit in myunits.items():
            lbl = mgr.get_unit(tag)
            self.assertEqual(unit, lbl)

    def test_multiple_threads(self):
        # launch two unit managers and collect the data for each
        # in two separate threads
        threads = []
        for ix in range(3):
            myunits = self.units.copy()
            thd = threading.Thread(target=self.collect_params, args=(myunits, ix), daemon=True)
            thd.start()
            threads.append(thd)

        for t in threads:
            t.join()


if __name__ == '__main__':
    unittest.main()

"""
Provides a class that maitains a list of units for the SICS parameters.
"""
import threading
import zmq
import time
import json

from queue import Queue
import xml.etree.ElementTree as et


class UnitManager(object):

    def __init__(self, sics, port, recv_wait=1000, ident='kafka'):

        self.units = {}
        self.rebuild = Queue()
        self.lock = threading.Lock()
        self.recv_wait = recv_wait
        self.end_point = 'tcp://{}:{}'.format(sics, port)
        self.socket = None
        self.transactions = 0
        self.ident = ident

        # create the thread that will rebuild the units
        self.thread = threading.Thread(target=self.run_update, daemon=True)
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
        print('zmq.DEALER {} connection to {}'.format(
            self.ident, self.end_point))

    def extract_units(self, xmls):
        root = et.fromstring(xmls)
        #print('\n' + xmls)

        try:
            unode = root.find('./component/property[@id="units"]/value')
            return unode.text
        except (AttributeError, TypeError):
            # print(xmls)
            return ''

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
            print('ZeroMQ: {}'.format(str(exc)))
            print('Re-establishing connection because of the failure...')
            self.open_connection(recover=True)
        return {}

    def recover_unit(self, tag):

        # get the response and parse the xml data
        response = self.cmd_request('SICS', 'getgumtreexml ' + tag)
        try:
            if response['flag'].lower() != 'ok':
                return ''
            return self.extract_units(response['reply'])
        except Exception as exc:
            # print(exc)
            return ''

    def run_update(self):
        # run forever after connection processing the message queue requests
        self.open_connection()
        while True:
            # get the message from the queue
            tag = self.rebuild.get()
            if tag is None:
                continue
            unit = self.recover_unit(tag)
            with self.lock:
                self.units[tag] = unit
            self.rebuild.task_done()

    def wait_for_update(self):
        # blocks until the queue is empty
        self.rebuild.join()

    def rebuild_units(self):
        # force a rebuild of all the units in the dictionary
        with self.lock:
            keys = self.units.keys()
            self.units = {}
        for p in keys:
            self.rebuild.put(p)

    def clear_units(self):
        # wait for the queue to empty and clear the dictionary
        self.rebuild.join()
        with self.lock:
            self.units = {}

    def add_unit(self, tag):

        with self.lock:
            skip = tag in self.units
            if not skip:
                # add an empty value to avoid multiple requests
                self.units[tag] = ''
        if not skip:
            self.rebuild.put(tag)

    def get_unit(self, tag):
        try:
            with self.lock:
                unit = self.units[tag]
            return unit
        except KeyError:
            return ''

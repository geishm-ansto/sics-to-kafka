
"""
Provides a class that manages the state events from the SICS stream. It is used
to issue a write command to the kafka to nexus writer.

"""
import threading
import zmq
import time
import json

from queue import Queue
import xml.etree.ElementTree as et

from cmdbuilder import CommandBuilder

class XmlParser(object):
    pass


class StateProcessor(object):
    """
    Implements a state machine that responds to incoming events (json dictionaries).
    The events are queued and processed in a separate thread. 
    """
    def __init__(self, sics, port, basefile, recv_wait=1000, ident='kafka'):

        self.basefile = basefile
        self.event_queue = Queue()
        self.lock = threading.Lock()
        self.recv_wait = recv_wait
        self.end_point = 'tcp://{}:{}'.format(sics, port)
        self.socket = None
        self.transactions = 0
        self.ident = ident
        self.open_connection()

        # create the thread that will rebuild the units
        self.thread = threading.Thread(target=self.process_events, daemon=True)
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

    def parse_xmls(self, xmls):
        # takes the xml string and returns a list of entries to be added
        # to the command builder
        # return a list values, stream, links

        pass
    
    def start_scan(self, ts):

        # requests the parameters to be saved from SICS
        resp = self.cmd_request('SICS', 'getgumtreexml /')
        if resp and resp['flag'].lower() == 'ok':
            pass # ok
        else:
            # trace message or exception
            raise ValueError('Unable to recover gumtreexml')
    
        # load the default command string for the instrument
        builder = CommandBuilder()
        builder.base_command(self.basefile, ts)
        
        # add the parameters from SICS


    def extract_units(self, xmls):
        root = et.fromstring(xmls)
        # print('\n' + xmls)

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

    def process_events(self):
        while True:
            # get the message from the queue
            resp = self.event_queue.get()
            if resp is None:
                continue
            
            # do something useful
            if resp["type"] == "State" and resp["name"] == "STARTED" and resp["value"] == "hmscan":
                self.start_scan(resp["ts"])

            self.event_queue.task_done()

    def wait_for_update(self):
        # blocks until the queue is empty
        self.event_queue.join()

    def add_event(self, event):
        self.event_queue.put(event)

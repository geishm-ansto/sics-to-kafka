
import copy
import threading

from json import loads
from kafka import KafkaConsumer
from streaming_data_types.status_x5f2 import Status as x5f2

from sicsclient.helpers import setup_module_logger
logger = setup_module_logger('sk.writer')
class WriterStatus:
    '''
    '''
    def __init__(self, kafka_broker='localhost:9092', status_topic='TEST_writerStatus'):
        self._lock = threading.Lock()
        self._is_idle = threading.Event()
        self._job_id = ""
        self._consumer = KafkaConsumer(bootstrap_servers=kafka_broker, auto_offset_reset='latest',
                                       enable_auto_commit=True)
        self._consumer.subscribe([status_topic])
        self._consumer.poll()

        # create the thread that will monitor the writer stats
        self.thread = threading.Thread(target=self.process, daemon=True)
        self.thread.start()

    def get_active_job(self):
        with self._lock:
            job_id = copy.copy(self._job_id) if self._job_id else ""
        return job_id

    def wait_for_idle(self, timeout):
        return self._is_idle.wait(timeout)

    def process(self):
        while True:
            msg_packets = self._consumer.poll(timeout_ms=10000)
            if msg_packets:
                for _, messages in msg_packets.items():
                    for msg in messages:
                        event = x5f2.Status.GetRootAsStatus(bytearray(msg.value), 0)
                        status = loads(event.StatusJson())
                        with self._lock:
                            self._job_id = str(status['job_id'])
                            if self._job_id:
                                self._is_idle.clear()
                            else:
                                self._is_idle.set()
            else:
                logger.warning('NX writer status timed out')
            
    # unit test support
    @property 
    def consumer(self):
        return self._consumer

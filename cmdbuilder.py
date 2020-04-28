import json
import uuid
import numpy as np
from datetime import datetime, timedelta
from collections.abc import Iterable
from helpers import unix_time_milliseconds


class CommandBuilder(object):
    def __init__(self, json_file=None):
        if json_file:
            with open(json_file, 'r') as fp:
                self.command = json.loads(fp.read())
        else:
            self.command = {}

    def base_command(self, filename, start_time, duration=3600, root='entry1', broker='localhost:9092'):
        """
        Starting point for the command structure
        """
        if start_time:
            start = start_time
        else:
            start = datetime.utcnow()
            start = int(unix_time_milliseconds(start))
        start_str = str(start)
        job_id = str(uuid.uuid4())

        self.command = {
            "cmd": "FileWriter_new",
            "job_id": job_id,
            "broker": broker,
            "start_time": start_str,
            "service_id": "filewriter1",
            "abort_on_uninitialised_stream": False,
            "use_hdf_swmr": True,
            "file_attributes": {
                "file_name": filename
            }
        }
        self.command["nexus_structure"] = { "children": [self._hdf_group(root, [("NX_class", "NXentry")])] }
        if duration:
            stop = start + 1000 * duration
            self.command["stop_time"] = str(stop)

    def add_stream(self, name, topic, source, writer, dtype, attributes):
      # assumes the name is the full path separated by '/' excluding the root entry
      npath = name.split('/')
      rnode = self.get_root()
      npath.insert(0, rnode["name"])

      # find the path and create if necessary
      pnode = self._find_node(rnode, npath[:-1], create=True)
      node = self._hdf_stream(npath[-1], topic, source, writer, dtype, attributes)
      pnode["children"].append(node)

    def set_param(self, filename=None, start_time=None, duration=0, root=None, broker=None, job_id=None):
        if filename:
            try:
                fa = self.command["file_attributes"]
                fa["filename"] = filename
            except KeyError: 
                self.command["file_attributes"] = {
                    "file_name": filename
                }
        if start_time:
            # passed in as unix time in ms
            self.command["start_time"] = str(start_time)
        if duration:
            #passed in as secs
            stop = int(self.command["start_time"]) + duration * 1000
            self.command["stop_time"] = str(stop)
        if root:
            node = self.get_root()
            if node:
                node["name"] = root
            else:
                self.command["nexus_structure"] = { "children": [self._hdf_group(root, [("NX_class", "NXentry")])] }
        if broker:
            self.command["broker"] = broker
        if job_id:
            self.command["job_id"] = job_id

    def add_dataset(self, name, values, dtype, attributes):
      # assumes the name is the full path separated by '/' excluding the root entry
      npath = name.split('/')
      rnode = self.get_root()
      npath.insert(0, rnode["name"])

      # find the path and create if necessary
      pnode = self._find_node(rnode, npath[:-1], create=True)
      node = self._hdf_dataset(npath[-1], values, dtype, attributes)
      pnode["children"].append(node)

    def as_json(self):
      return json.dumps(self.command, indent=4)

    def save(self, ofile):
        jstr = self.as_json()
        with open(ofile, 'w') as f:
            f.write(jstr)

    def get_command(self):
      return self.command.copy()

    def get_job_id(self):
        return self.command["job_id"]

    def _hdf_group(self, name, attributes):

        el = {
            "type": "group",
            "name": name,
            "children": [
            ]
        }
        if attributes:
            el['attributes'] = [{'name': k, 'values': v}
                                for k, v in attributes]
        return el

    def _hdf_stream(self, name, topic, source, writer, dtype, attributes):

        el = {
            "type": "group",
            "name": name,
            "children": [
                {
                    "type": "stream",
                    "stream": {
                        "dtype": dtype,
                        "topic": topic,
                        "source": source,
                        "writer_module": writer
                    }
                }
            ]
        }
        if attributes:
            el['attributes'] = [{'name': k, 'values': v}
                                for k, v in attributes]
        return el

    def _nested_list_shape(self, values):
        # recover the shape by testing length of first element
        # need to handle strings as they have a length
        shape = []
        v = values
        while True:
            try:
                if not isinstance(v, list):
                    break
                shape.append(len(v))
                v = v[0]
            except (IndexError, TypeError):
                break
        return shape

    def _hdf_dataset(self, name, values, dtype, attributes):

        el = {
            "type": "dataset",
            "name": name,
            "dataset": {
                "type": dtype
            }
        }

        # need to handle single values or nested lists or numpy arrays
        # start with numpy objects
        if isinstance(values, np.ndarray):
            el["dataset"]["size"] = list(values.shape)
            el["values"] = values.tolist()
        elif isinstance(values, list):
            # assume it may be a nested list
            el["dataset"]["size"] = self._nested_list_shape(values)
            el["values"] = values
        else:
            el["values"] = values

        if attributes:
            el['attributes'] = [{'name': k, 'values': v}
                                for k, v in attributes]
        return el

    def _find_node(self, cnode, npath, create=False):
        # assumes path is separated by '/' and that it was split 
        # prior to the call and that it is 
        if cnode["name"] == npath[0] or npath[0] == ".":
          # if it is the end of the path return the node
          if len(npath) == 1:   
            return cnode
          # check the childrene for the next level
          if cnode["type"] == "group":
            for node in cnode["children"]:
              rnode = self._find_node(node, npath[1:], create=create)
              if rnode:
                return rnode   
            # next level failed but add if it should be created and pass it down
            if create:
              node = self._hdf_group(npath[1], [("NX_class", "NXgroup")])
              cnode["children"].append(node)
              return self._find_node(node, npath[1:], create=create)

        # no matches if it got here 
        return None

    def get_root(self):
        try:
            return self.command["nexus_structure"]["children"][0]
        except (KeyError, IndexError):
            None

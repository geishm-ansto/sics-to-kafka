import json
import uuid
import numpy as np
from datetime import datetime
from sicsclient.helpers import unix_time_milliseconds


class CommandBuilder(object):
    def __init__(self, json_file=None, name_stream=False):
        self.name_stream = name_stream  # enabled for unit testing
        if json_file:
            with open(json_file, 'r') as fp:
                self.command = json.loads(fp.read())
        else:
            self.command = {}

    def base_command(self, filename, start_time_ms, stop_time_ms=None, root='entry1', broker='localhost:9092'):
        """
        Starting point for the command structure
        """
        if start_time_ms:
            start = int(start_time_ms)
        else:
            start = datetime.utcnow()
            start = int(unix_time_milliseconds(start))
        job_id = str(uuid.uuid4())

        self.command = {
            "cmd": "FileWriter_new",
            "job_id": job_id,
            "broker": broker,
            "start_time": start,
            "service_id": "filewriter1",
            "abort_on_uninitialised_stream": False,
            "use_hdf_swmr": True,
            "filename": filename
        }
        self.command["nexus_structure"] = {"children": [
            self._hdf_group(root, [("NX_class", "NXentry")])]}
        if stop_time_ms:
            self.command["stop_time"] = int(stop_time_ms)

    def add_stream(self, name, topic, source, writer, dtype, values, attributes):
        # assumes the name is the full path separated by '/' excluding the root entry
        npath = name.split('/')
        rnode = self.get_root()
        npath.insert(0, rnode["name"])

        # find the path and create if necessary
        pnode = self._find_node(rnode, npath[:-1], create=True)
        if writer == "sval":
            node = self._hdf_sval_stream(npath[-1], topic, source, dtype, values, attributes)
        else:
            node = self._hdf_stream(
                npath[-1], topic, source, writer, dtype, values, attributes)
        pnode["children"].append(node)

    def set_param(self, filename=None, start_time_ms=None, stop_time_ms=None, root=None, broker=None, job_id=None):
        if filename:
            self.command["filename"] = filename
        if start_time_ms is not None:
            # passed in as unix time in ms
            self.command["start_time"] = int(start_time_ms)
        if stop_time_ms is not None:
            # passed in as unix time in ms
            self.command["stop_time"] = int(stop_time_ms)
        if root:
            node = self.get_root()
            if node:
                node["name"] = root
            else:
                self.command["nexus_structure"] = {"children": [
                    self._hdf_group(root, [("NX_class", "NXentry")])]}
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

    def get_value(self, tag):
        try:
            return self.command[tag]
        except KeyError:
            return None

    def get_command(self):

        tags = ['job_id', 'start_time', 'stop_time', 'broker', 'filename', 'service_id']
        cmd = {}
        for tag in tags:
            try:
                cmd[tag] = self.command[tag]
            except KeyError:
                pass
        try:
            cmd['nexus_structure'] = json.dumps(self.command['nexus_structure'])
        except KeyError:
            pass
        
        return cmd

    def get_start_time(self):
        # epoch time in msec
        try:
            return int(self.command["start_time"])
        except (ValueError,KeyError):
            return None

    def get_stop_time(self):
        # epoch time in msec
        try:
            return int(self.command["stop_time"])
        except (ValueError,KeyError):
            return None

    def get_job_id(self):
        try:
            return self.command["job_id"]
        except KeyError:
            return None

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

    def _hdf_stream(self, name, topic, source, writer, dtype, values, attributes):

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
        if values is not None:
            _, target = self._map_values(values)
            el["children"][0]["stream"]["initial_value"] = target

        if attributes:
            el['attributes'] = [{'name': k, 'values': v}
                                for k, v in attributes]
        return el

    def _hdf_sval_stream(self, name, topic, source, dtype, values, attributes):

        el = {
            "type": "stream",
            "stream": {
                "dtype": dtype,
                "name": name,
                "topic": topic,
                "source": source,
                "writer_module": "sval"
            }
        }
        if self.name_stream:
            el["name"] = name   # debugging
        if values is not None:
            _, target = self._map_values(values)
            el["stream"]["initial_value"] = target
        if attributes:
            addnl_attr = []
            for k, v in attributes:
                if k == "units":
                    el["stream"]["value_units"] = v
                else:
                    addnl_attr.append({'name': k, 'values': v})
            if addnl_attr:
                el["attributes"] = addnl_attr
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

    def _map_values(self, values):
        # need to handle single values or nested lists or numpy arrays
        # start with numpy objects
        size = None
        target = None
        if isinstance(values, np.ndarray):
            size = list(values.shape)
            target = values.tolist()
        elif isinstance(values, list):
            # assume it may be a nested list
            size = self._nested_list_shape(values)
            target = values
        else:
            target = values
        return size, target

    def _hdf_dataset(self, name, values, dtype, attributes):

        el = {
            "type": "dataset",
            "name": name,
            "dataset": {
                "type": dtype
            }
        }

        size, target = self._map_values(values)
        if size:
            el["dataset"]["size"] = size
        el["values"] = target

        if attributes:
            el['attributes'] = [{'name': k, 'values': v}
                                for k, v in attributes]
        return el

    def _find_node(self, cnode, npath, create=False):
        # assumes path is separated by '/' and that it was split
        # prior to the call
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
                    node = self._hdf_group(npath[1], [("NX_class", "NXdata")])
                    #node = self._hdf_group(npath[1], [])
                    cnode["children"].append(node)
                    return self._find_node(node, npath[1:], create=create)

        # no matches if it got here
        return None

    def get_root(self):
        try:
            return self.command["nexus_structure"]["children"][0]
        except (KeyError, IndexError):
            None

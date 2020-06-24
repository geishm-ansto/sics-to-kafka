#!/usr/bin/python
"""
  A collection of useful routines to extract the structure from existing nexus files,
  build the commands for starting, stopping and initiating a time scan.
"""

import json
import h5py
import uuid
import logging
from datetime import datetime, timedelta
from collections.abc import Iterable

def get_module_logger(mod_name):
    """
    To use this, do logger = get_module_logger(__name__)
    """
    logger = logging.getLogger(mod_name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger

def unix_time_milliseconds(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


def extract_structure(filename, topic):
    """
    Returns a dictionary that maps to the nexus structure required by the 
    kafka-to-nexus writer from the ESS project
    """

    def invoke_dump(elem):
        try:
            return (map_call[type(elem)])(elem)
        except KeyError:
            print('Cannot find {} : {}'.format(elem.name, type(elem)))
            return None

    def dump_group(elem):

        tags = elem.name.split('/')
        msg = {}
        msg['type'] = 'group'
        msg['name'] = tags[-1]
        msg['children'] = []
        for k, v in elem.items():
            msg['children'].append(invoke_dump(v))
        if len(elem.attrs):
            attr = []
            for key, value in elem.attrs.items():
                attr.append({'name': key, 'values': value.decode('utf-8')})
            msg['attribute'] = attr
        return msg

    def dump_dataset(elem):
        # dataset needs type:, name:, dataset: { type:, [size:]}, [attributes: []]
        # exception: if attr['target'] exists and does not equal elem.name this is a
        # link
        tags = elem.name.split('/')
        msg = {}
        msg['type'] = 'stream'
        msg['name'] = tags[-1]

        attr = []
        if len(elem.attrs):
            for key, value in elem.attrs.items():

                # check if satisfies condition for a link
                svalue = value.decode('utf-8')
                if key == 'target' and svalue != elem.name:
                    msg['type'] = 'link'
                    msg['target'] = svalue
                    return msg

                attr.append({'name': key, 'values': value.decode('utf-8')})

        stream = {}
        stype = str(elem.dtype)
        stream['dtype'] = 'string' if stype.startswith('|S') else stype
        stream['writer_module'] = 'f142'
        stream['source'] = elem.name
        stream['topic'] = topic
        try:
            if elem.shape[0] == 1:
                stream['writer_module'] = 'sval'
        except AttributeError:
            pass
        msg['stream'] = stream

        if attr:
            msg['attribute'] = attr

        return msg

    map_call = {
        h5py._hl.group.Group: dump_group,
        h5py._hl.dataset.Dataset: dump_dataset
    }

    roots = []
    with h5py.File(filename, 'r') as fp:
        for k, v in fp.items():
            roots.append(invoke_dump(v))
    msg = {'children': [roots]}
    return msg


def save_structure(hdfsource, filename, topic):
    """
    Extracts the structure from an existing file and saves the format to the
    filename in a json format.
    """
    schema = extract_structure(hdfsource, topic)
    json_str = json.dumps(schema, indent=4)
    with open(filename, 'w') as f:
        f.write(json_str)

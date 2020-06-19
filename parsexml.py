#!/usr/bin/python

import os
import xmltodict

from collections import namedtuple, OrderedDict

Component = namedtuple(
    'Component', ['tag', 'value', 'dtype', 'klass', 'mutable', 'nxalias', 'units', 'nxsave'])


def get_properties(node):

    properties = {}
    try:
        for pd in node['property']:
            properties[pd['@id']] = pd['value']
    except (KeyError, AttributeError, TypeError):
        raise ValueError('Missing component properties')

    return properties


def get_component(node, folder):

    def get_value(pps, tag, def_value):
        if tag in pps:
            return pps[tag]
        else:
            return def_value

    props = get_properties(node)
    if props:
        try:
            if 'data' in props and props['data'] == 'true':
                tag = folder + '/' + node['@id'] if folder else node['@id']
                dtype = node['@dataType']
                if dtype == 'int':
                    value = int(node['value'])
                elif dtype == 'float':
                    value = float(node['value'])
                else:
                    value = str(node['value'])
                klass = props['klass']
                mutable = get_value(props, 'mutable', '') == 'true'
                units = get_value(props, 'units', '')
                nxsave = get_value(props, 'nxsave', '') == 'true'
                nxalias = '/'.join(get_value(props, 'nxalias', '').split('_'))
                cmp = Component(tag, value, dtype, klass, mutable, nxalias, units, nxsave)
                return cmp
        except KeyError:
            raise ValueError('Property error for {}'.format(folder))
    else:
        return None


def parse(clist, tag, nodes, folder):

    # receives the component list, current component node and node hierarchy
    # only interested in components
    if tag != 'component':
        return
    # a component is either a list of child components or a single OrderedDict
    if isinstance(nodes, list):
        for node in nodes:
            # if the node is of type {int, float, text} then create the component end point
            # else if type is {none} it is a group so continue
            try:
                dtype = node['@dataType']
            except (TypeError, KeyError, IndexError):
                print('{}: {}'.format(tag, folder))
                return
            if dtype in ['int', 'float', 'text']:
                cmp = get_component(node, folder)
                if cmp:
                    clist.append(cmp)
            elif dtype == 'none':
                # append the folder names
                nfold = folder + '/' + node['@id'] if folder else node['@id']
                for k, v in node.items():
                    parse(clist, k, v, nfold)
            else:
                raise ValueError('Unexpected data type: {}'.format(dtype))
    elif isinstance(nodes, OrderedDict):
        nfold = folder + '/' + nodes['@id'] if folder else nodes['@id']
        for k, v in nodes.items():
            parse(clist, k, v, nfold)
    else:
        raise ValueError('Unexpected component type: {}'.format(type(nodes)))


def parsesics(xmlstr):

    doc = xmltodict.parse(xmlstr)

    # the root level is hipdaba:SICS
    root = doc['hipadaba:SICS']
    clist = []
    folder = ''

    for k, v in root.items():
        parse(clist, k, v, folder)

    return clist


#!/usr/bin/python
"""
  A little helper program for generating the python serialisers and 
  deserialisers for the schemas used in BrightnESS/ESS streaming.

  Mark Koennecke, December 2018
    
"""
import glob
import subprocess

#===================================================================
# Configuration section
#===================================================================
schemapath = '../streaming-data-types/schemas/'

#============= Do Something ............... ========================

schemas = glob.glob(schemapath + 'f142*.fbs')
schemas += glob.glob(schemapath + 'json_json.fbs')

combase = ['../flatbuffers/flatc', '--python','-I','%s/schemas' %(schemapath),'-o','pyschema']
for schema in schemas:
    com = combase
    com.append(schema)
    subprocess.call(com)

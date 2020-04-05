# automatically generated by the FlatBuffers compiler, do not modify

# namespace: 

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class UInt(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsUInt(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = UInt()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def UIntBufferHasIdentifier(cls, buf, offset, size_prefixed=False):
        return flatbuffers.util.BufferHasIdentifier(buf, offset, b"\x66\x31\x34\x32", size_prefixed=size_prefixed)

    # UInt
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # UInt
    def Value(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint32Flags, o + self._tab.Pos)
        return 0

def UIntStart(builder): builder.StartObject(1)
def UIntAddValue(builder, value): builder.PrependUint32Slot(0, value, 0)
def UIntEnd(builder): return builder.EndObject()

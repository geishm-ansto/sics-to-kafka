# automatically generated by the FlatBuffers compiler, do not modify

# namespace: 

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class RunStop(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsRunStop(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = RunStop()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def RunStopBufferHasIdentifier(cls, buf, offset, size_prefixed=False):
        return flatbuffers.util.BufferHasIdentifier(buf, offset, b"\x36\x73\x34\x74", size_prefixed=size_prefixed)

    # RunStop
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # RunStop
    def StopTime(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint64Flags, o + self._tab.Pos)
        return 0

    # RunStop
    def RunName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStop
    def JobId(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStop
    def ServiceId(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # RunStop
    def CommandId(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

def RunStopStart(builder): builder.StartObject(5)
def RunStopAddStopTime(builder, stopTime): builder.PrependUint64Slot(0, stopTime, 0)
def RunStopAddRunName(builder, runName): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(runName), 0)
def RunStopAddJobId(builder, jobId): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(jobId), 0)
def RunStopAddServiceId(builder, serviceId): builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(serviceId), 0)
def RunStopAddCommandId(builder, commandId): builder.PrependUOffsetTRelativeSlot(4, flatbuffers.number_types.UOffsetTFlags.py_type(commandId), 0)
def RunStopEnd(builder): return builder.EndObject()

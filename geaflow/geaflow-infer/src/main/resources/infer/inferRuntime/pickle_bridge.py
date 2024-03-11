import pickle
import struct
from mmap_ipc import PyJavaIPC

class PicklerDataBridger(object):

    def __init__(self, input_queue_shm_key, output_queue_shm_key, input_size):
        self.data_bridge = PyJavaIPC(output_queue_shm_key.encode('utf-8'), input_queue_shm_key.encode('utf-8'))
        self.input_size = input_size

    def read_data(self):
        data_head = self.data_bridge.readBytes(4)
        if not data_head:
            return None
        data_len, = struct.unpack("<i", data_head)
        data_ = self.data_bridge.readBytes(data_len)
        args_bytes = data_[:4]
        args_size, = struct.unpack("<i", args_bytes)
        inputs = []
        start = 4
        for i in range(args_size):
            data_args_bytes = data_[start:start + 4]
            data_le, = struct.unpack("<i", data_args_bytes)
            start = start + 4
            le_ = data_[start:start + data_le]
            loads = pickle.loads(le_)
            start = start + data_le
            inputs.append(loads)
        return inputs

    def write_data(self, data):
        data_bytes = pickle.dumps(data)
        data_len = len(data_bytes)
        data_len_bytes = struct.pack("<i", data_len)
        flag0 = self.data_bridge.writeBytes(data_len_bytes, 4)
        if flag0 is False:
            return False
        flag1 = self.data_bridge.writeBytes(data_bytes, data_len)
        return flag1

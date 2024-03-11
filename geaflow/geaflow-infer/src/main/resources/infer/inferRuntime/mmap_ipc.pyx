# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: annotate = True

cimport cython
from libcpp cimport bool
from libc.stdint cimport *


cdef extern from "MmapIPC.h":
    cdef cppclass MmapIPC:
        MmapIPC(char* , char*) except +
        int readBytes(int) nogil except +
        bool writeBytes(char *, int) nogil except +
        bool ParseQueuePath(string, string, long *)
        uint8_t* getReadBufferPtr()


@cython.final
cdef class PyJavaIPC:

    cdef MmapIPC * ipcBridge
    cdef uint8_t* readPtr;

    def __cinit__(self, r, w):
        self.ipcBridge = new MmapIPC(r, w)
        self.readPtr = self.ipcBridge.getReadBufferPtr()

    cpdef inline bytes readBytes(self, bytesSize):
        if bytesSize == 0:
            return b""
        cdef int readSize
        cdef int len_ = bytesSize
        with nogil:
            readSize = self.ipcBridge.readBytes(len_)
        if readSize == 0:
            return b""
        cdef unsigned char * binary_data = self.readPtr
        return binary_data[:len_]

    cpdef inline bool writeBytes(self, bytesBuf, length):
        cdef bool writeFlag
        cdef int len_ = length
        cdef char* buf_ = bytesBuf
        with nogil:
            writeFlag = self.ipcBridge.writeBytes(buf_, len_)
        return writeFlag

    def __dealloc__(self):
        del self.ipcBridge

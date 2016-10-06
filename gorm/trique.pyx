cdef class TriqueEntry:
    cdef public TriqueEntry next, prev
    cdef public object value
    
    def __cinit__(self, object value, TriqueEntry prev=None, TriqueEntry nxt=None):
        self.value = value
        self.next = nxt
        self.prev = prev


cdef class Trique:
    cdef TriqueEntry head, waist, tail
    cdef int length

    def __cinit__(self, list data=[]):
        self.length = 0
        self.head = None
        self.waist = None
        self.tail = None
        for datum in data:
            self.append(datum)

    def __len__(self):
        return self.length

    cdef seek(self, int n=0):
        if n == 0:
            return
        if self.waist is None:
            if self.head is None:
                raise IndexError("nothing to seek through")
            self.waist = self.head
        while n > 0:
            if self.waist.next is None:
                raise IndexError("seek past end of trique")
            self.waist = self.waist.next
            n -= 1
        while n < 0:
            if self.waist.prev is None:
                raise IndexError("seek past start of trique")
            self.waist = self.waist.prev
            n += 1
    
    cdef appendentry(self, TriqueEntry entry):
        if self.head is None:
            entry.next = entry.prev = None
            self.head = self.tail = entry
            return
        entry.prev = self.tail
        entry.next = None
        self.tail.next = entry
        self.tail = entry
        self.length += 1

    cpdef append(self, object value):
        self.appendentry(TriqueEntry(value))

    cdef appendleftentry(self, TriqueEntry entry):
        if self.head is None:
            entry.next = entry.prev = None
            self.head = self.tail = entry
            return
        entry.next = self.head
        entry.prev = None
        self.head.prev = entry
        self.head = entry
        self.length += 1

    cpdef appendleft(self, object value):
        self.appendleftentry(TriqueEntry(value))

    cdef TriqueEntry popentry(self):
        cdef TriqueEntry ret = self.tail
        if self.tail.prev is None:
            self.head = self.tail = None
        else:
            self.tail = self.tail.prev
        self.length -= 1
        if ret is self.waist:
            self.waist = self.waist.prev or self.waist.next
        return ret

    cdef TriqueEntry popleftentry(self):
        cdef TriqueEntry ret = self.head
        if ret.next is None:
            self.head = self.tail = None
        else:
            self.head = ret.next
        self.length -= 1
        if ret is self.waist:
            self.waist = self.waist.next or self.waist.prev
        return ret

    cdef TriqueEntry popentry(self, int i=-1):
        cdef TriqueEntry ret, prev, nxt
        if i == 0:
            return self.popleftentry()
        elif i == -1:
            return self.popentry()
        elif i > 0:
            self.waist = self.head
            self.seek(i)
        else:  # i < 0
            self.waist = self.tail
            self.seek(i+1)
        ret = self.waist
        prev = ret.prev
        nxt = ret.next
        prev.next = nxt
        nxt.prev = prev
        self.waist = prev if i < 0 else nxt
        self.length -= 1
        return ret

    cpdef object pop(self, int i=-1):
        return self.popentry(i).value

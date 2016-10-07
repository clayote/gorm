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

    @property
    def middle(self):
        if self.waist is None:
            return None
        return self.waist.value

    @middle.setter
    def middle(self, object v):
        if self.waist is None:
            self.waist.value = v

    def __cinit__(self, list data=[]):
        self.length = 0
        self.head = None
        self.waist = None
        self.tail = None
        self.extend(data)

    def __len__(self):
        return self.length

    def __iter__(self):
        cdef TriqueEntry here = self.head
        while here is not None:
            yield here
            here = here.next

    cdef TriqueEntry seekentry(self, int n=0):
        if n == 0:
            return self.waist
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
        return self.waist

    cpdef object seek(self, int n=0):
        return self.seekentry(n).value

    cdef TriqueEntry getentry(self, int i=0):
        if i >= 0:
            self.waist = self.head
        elif i <= -1:
            self.waist = self.tail
            i += 1
        self.seek(i)
        return self.waist

    def __getitem__(self, int i=0):
        return self.getentry(i).value

    def __setitem__(self, int i, object v):
        if i == self.length:
            self.append(v)
        elif i > self.length:
            raise IndexError("Set past end of trique")
        elif i == 0:
            if self.length == 0:
                self.append(v)
            else:
                self.head.value = v
        elif i == -1 or i == self.length - 1:
            if self.length == 0:
                self.append(v)
            else:
                self.tail.value = v
        else:
            if i > 0:
                self.waist = self.head
            else:  # i < 0
                self.waist = self.tail
                i += 1
            self.seek(i)
            self.waist.value = v

    def __delitem__(self, int i):
        if i == 0:
            self.head = self.head.next
            self.head.prev = None
        elif i == -1 or i == self.length - 1:
            self.tail = self.tail.prev
            self.tail.next = None
        elif i >= self.length:
            raise IndexError("del past end of trique")
        else:
            if i > 0:
                self.waist = self.head
            else:  # i < 0
                self.waist = self.tail
                i += 1
            self.seek(i)
            self.waist.prev.next = self.waist.next
            self.waist.next.prev = self.waist.prev
            self.waist = self.waist.prev

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

    cpdef extend(self, object iterable):
        cdef TriqueEntry nxt, prev
        for obj in iterable:
            if nxt is None:
                prev = TriqueEntry(obj, self.tail)
                if self.head is None:
                    self.head = self.tail = prev
            else:
                nxt = TriqueEntry(obj, prev)
                prev.next = nxt
        if nxt is None:
            return
        if prev is None:
            self.appendentry(prev)
        else:
            self.tail = nxt

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

    cdef insertmiddleentry(self, TriqueEntry entry):
        cdef TriqueEntry nxt = self.waist.next
        self.waist.next = entry
        nxt.prev = entry
        entry.prev = self.waist
        entry.next = nxt
        self.waist = entry

    cpdef insertmiddle(self, object v):
        cdef TriqueEntry newentry = TriqueEntry(v, self.waist, self.waist.next)
        self.waist.next.prev = newentry
        self.waist.next = newentry
        self.waist = newentry

    cpdef insert(self, int i, object v):
        self.seek(i)
        self.insertmiddle(v)

    cpdef appendleft(self, object value):
        self.appendleftentry(TriqueEntry(value))

    cdef TriqueEntry poprightentry(self):
        cdef TriqueEntry ret = self.tail
        if self.tail.prev is None:
            self.head = self.tail = None
        else:
            self.tail = self.tail.prev
        if ret is self.waist:
            self.waist = self.waist.prev or self.waist.next
        self.length -= 1
        return ret

    cdef TriqueEntry popleftentry(self):
        cdef TriqueEntry ret = self.head
        if ret.next is None:
            self.head = self.tail = None
        else:
            self.head = ret.next
        if ret is self.waist:
            self.waist = self.waist.next or self.waist.prev
        self.length -= 1
        return ret

    cdef TriqueEntry popentry(self, int i=-1):
        cdef TriqueEntry ret, prev, nxt
        if i == 0:
            return self.popleftentry()
        elif i == -1:
            return self.poprightentry()
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

    cpdef object popleft(self):
        return self.popleftentry().value

    cdef TriqueEntry popmiddleentry(self, int n=0):
        cdef TriqueEntry ret, prev, nxt
        if n != 0:
            self.seek(n)
        ret = self.waist
        prev = self.waist.prev
        nxt = self.waist.next
        prev.next = nxt
        nxt.prev = prev
        self.length -= 1
        return ret

    cpdef object popmiddle(self, int n=0):
        return self.popmiddleentry(n).value

cdef class QueueEntry:
    cdef readonly int rev
    cdef readonly object value
    cdef QueueEntry prev, next

    def __cinit__(self, int rev, object value, QueueEntry prev=None, QueueEntry next=None):
        self.rev = rev
        self.value = value
        self.prev = prev
        self.next = next

    def __iter__(self):
        yield self.rev
        yield self.value

    def __len__(self):
        return 2

    def __getitem__(self, int i):
        if i == 0:
            return self.rev
        elif i == 1:
            return self.value
        else:
            raise IndexError


cdef class Queue:
    cdef QueueEntry head, tail

    def __cinit__(self, QueueEntry head=None, QueueEntry tail=None):
        self.head = head
        self.tail = tail

    def __getitem__(self, int i):
        if self.head is None or self.tail is None:
            raise IndexError
        if i == 0:
            return self.head
        if i == -1:
            return self.tail
        cdef QueueEntry entry
        if i > 0:
            entry = self.head
            while i > 0:
                if entry.next is None:
                    raise IndexError
                entry = entry.next
                i -= 1
            return entry
        if i < -1:
            entry = self.tail
            while i < -1:
                if entry.prev is None:
                    raise IndexError
                entry = entry.prev
                i += 1
            return entry

    def __setitem__(self, int i, tuple rv):
        cdef int rev
        cdef QueueEntry entry
        rev, value = rv
        if i == 0:
            if self.head is None:
                self.head = self.tail = QueueEntry(rev, value)
            else:
                self.head.rev = rev
                self.head.value = value
        elif i == -1:
            if self.tail is None:
                self.head = self.tail = QueueEntry(rev, value)
            else:
                self.tail.rev = rev
                self.tail.value = value
        else:
            entry = self[i]
            entry.rev = rev
            entry.value = value

    def __delitem__(self, int i):
        self.pop(i)

    def __len__(self):
        cdef int n
        cdef QueueEntry entry
        if self.head is None:
            return 0
        entry = self.head
        n = 1
        while entry.next is not None:
            entry = entry.next
            n += 1
        return n

    def __iter__(self):
        cdef QueueEntry entry
        if self.head is None:
            return
        entry = self.head
        yield entry
        while entry.next is not None:
            entry = entry.next
            yield entry

    cdef clear(self):
        self.head = None
        self.tail = None

    cdef appendentry(self, QueueEntry new_entry):
        if self.tail is not None:
            self.tail.next = new_entry
            new_entry.prev = self.tail
        else:
            self.head = new_entry
        self.tail = new_entry

    cdef append(self, int rev, object value):
        cdef QueueEntry new_entry = QueueEntry(rev, value, self.tail)
        self.appendentry(new_entry)

    cdef appendleftentry(self, QueueEntry new_entry):
        if self.head is not None:
            self.head.prev = new_entry
            new_entry.next = self.head
        else:
            self.tail = new_entry
        self.head = new_entry

    cdef appendleft(self, int rev, object value):
        cdef QueueEntry new_entry = QueueEntry(rev, value, None, self.head)
        self.appendleftentry(new_entry)

    cdef QueueEntry _pop_left(self):
        cdef QueueEntry entry = self.head
        if entry is None:
            raise IndexError("pop from empty queue")
        self.head = entry.next
        if self.head is None:
            self.tail = None
        else:
            self.head.prev = None
        return entry

    cdef QueueEntry popleft(self, int i=0):
        cdef Queue popped
        if i > 0:
            popped = Queue()
            while i > 0:
                popped.appendentry(self.popleft())
                i -= 1
            ret = popped.pop()
            while popped.next is not None:
                self.appendleftentry(popped.pop())
            return ret
        elif i < 0:
            return self.pop(i)
        return self._pop_left()

    cdef QueueEntry _pop(self):
        cdef QueueEntry entry = self.tail
        if entry is None:
            raise IndexError("pop from empty queue")
        self.tail = entry.prev
        if self.tail is None:
            self.head = None
        else:
            self.tail.next = None
        return entry

    cdef QueueEntry pop(self, int i=-1):
        cdef Queue popped
        if i > -1:
            return self.popleft(i)
        elif i < -1:
            popped = Queue()
            while i < -1:
                popped.appendentry(self.pop())
                i += 1
            ret = popped.pop()
            while popped.next is not None:
                self.appendentry(popped.pop())
            return ret
        return self._pop()

    cdef QueueEntry peek(self):
        return self.tail

    cdef QueueEntry peekleft(self):
        return self.head


cdef class KeysView:
    cdef Queue _past, _future
    
    def __cinit__(self, Queue past, Queue future):
        self._past = past
        self._future = future

    def __iter__(self):
        for item in self._past:
            yield item.rev
        for item in self._future:
            yield item.rev
    
    def __len__(self):
        return len(self._past) + len(self._future)

    def __contains__(self, int rev):
        for item in self._past:
            if item.rev == rev:
                return True
        for item in self._future:
            if item.rev == rev:
                return True
        return False


cdef class ItemsView:
    cdef Queue _past, _future
    
    def __cinit__(self, Queue past, Queue future):
        self._past = past
        self._future = future

    def __iter__(self):
        yield from self._past
        yield from self._future

    def __len__(self):
        return len(self._past) + len(self._future)

    def __contains__(self, item):
        if isinstance(item, QueueEntry):
            return item in self._past or item in self._future
        for record in self._past:
            if record == item:
                return True
        for record in self._future:
            if record == item:
                return True
        return False


cdef class ValuesView:
    cdef Queue _past, _future
    
    def __cinit__(self, Queue past, Queue future):
        self._past = past
        self._future = future

    def __iter__(self):
        for item in self._past:
            yield item.value
        for item in self._future:
            yield item.value

    def __len__(self):
        return len(self._past) + len(self._future)

    def __contains__(self, v):
        for item in self._past:
            if item.value == v:
                return True
        for item in self._future:
            if item.value == v:
                return True
        return False


cdef class WindowDict:
    """A dict that keeps every value that a variable has had over time.
    
    Look up a revision number in this dict and it will give you the effective value as
    of that revision. Keys should always be revision numbers. Once a key is set, all greater
    keys are considered to be in this dict unless the value is ``None``. Keys after that one
    aren't "set" until one's value is non-``None`` again.
    
    Optimized for the cases where you look up the same revision repeatedly, or its neighbors.
    
    """
    cdef Queue _past, _future

    cpdef void seek(self, int rev):
        """Arrange the caches in the optimal way for looking up the given revision."""
        # TODO: binary search? Perhaps only when one or the other deque is very large?
        cdef int chkrev
        while len(self._past) > 0:
            chkrev, v = self._past.pop()
            if chkrev < rev:
                self._past.append(chkrev, v)
                break
            self._future.appendleft(chkrev, v)
        while len(self._future) > 0:
            chkrev, v = self._future.popleft()
            if chkrev > rev:
                self._future.appendleft(chkrev, v)
                break
            self._past.append(chkrev, v)

    cpdef int rev_before(self, int rev):
        """Return the last rev prior to the given one on which the value changed."""
        self.seek(rev)
        if len(self._past) == 0:
            raise KeyError
        return self._past[-1].rev

    cpdef int rev_after(self, int rev):
        """Return the next rev after the given one on which the value will change, or None if it never will."""
        self.seek(rev)
        if len(self._future) > 0:
            return self._future[0].rev

    cpdef KeysView keys(self):
        return KeysView(self._past, self._future)

    cpdef ItemsView items(self):
        return ItemsView(self._past, self._future)

    cpdef ValuesView values(self):
        return ValuesView(self._past, self._future)

    cpdef object get(self, int rev, object default=None):
        try:
            return self[rev]
        except KeyError:
            return default

    cpdef object setdefault(self, int rev, object default=None):
        try:
            return self[rev]
        except KeyError:
            self[rev] = default
            return default

    def update(self, E=None, **F):
        if E is not None:
            if hasattr(E, 'keys'):
                for k in E:
                    self[k] = E[k]
            else:
                for k, v in E:
                    self[k] = v
        for k, v in F.items():
            self[k] = v

    cpdef void clear(self):
        self._past.clear()
        self._future.clear()

    cpdef object pop(self, int rev):
        ret = self[rev]
        del self[rev]
        return ret

    cpdef tuple popitem(self, int rev):
        v = self[rev]
        del self[rev]
        return rev, v

    def __init__(self, dict data={}):
        self._past = Queue()
        for rev, v in sorted(data.items()):
            self._past.append(rev, v)
        self._future = Queue()

    def __iter__(self):
        for record in self._past:
            yield record.rev
        for record in self._future:
            yield record.rev

    def __contains__(self, int item):
        cdef int rev
        for rev, v in self._past:
            if rev == item:
                return True
        for rev, v in self._future:
            if rev == item:
                return True
        return False

    def __richcmp__(self, WindowDict other, int op):
        cdef Queue myhist, yourhist
        cdef QueueEntry myrec, yourrec
        if op not in (2, 3):
            raise TypeError
        myhist = Queue()
        yourhist = Queue()
        for record in self._past:
            myhist.appendentry(record)
        for record in self._future:
            myhist.appendentry(record)
        for record in other._past:
            yourhist.appendentry(record)
        for record in other._future:
            yourhist.appendentry(record)
        if op == 2:
            while myhist.head is not None:
                if yourhist.head is None:
                    return False
                myrec = myhist.popleft()
                yourrec = yourhist.popleft()
                if myrec != yourrec:
                    return False
            return yourhist.head is None
        if op == 3:
            if myhist.head != yourhist.head or myhist.tail != yourhist.tail:
                return True
            while myhist.head is not None:
                if yourhist.head is None:
                    return True
                myrec = myhist.popleft()
                yourrec = yourhist.popleft()
                if myrec != yourrec:
                    return True
            return yourhist.head is not None

    def __len__(self):
        return len(self._past) + len(self._future)

    def __getitem__(self, int rev):
        self.seek(rev)
        try:
            return self._past[-1].value
        except IndexError:
            raise KeyError

    def __setitem__(self, int rev, object v):
        cdef int pastrev
        if len(self._past) == 0 and len(self._future) == 0:
            self._past.append(rev, v)
        self.seek(rev)
        if len(self._past) == 0:
            self._past.append(rev, v)
            return
        pastrev = self._past[-1][0]
        if rev == pastrev:
            self._past[-1] = (rev, v)
        else:
            assert rev > pastrev
            self._past.append(rev, v)
    
    def __delitem__(self, int rev):
        cdef QueueEntry nothing = QueueEntry(rev, None)
        cdef QueueEntry tmp
        while len(self._past) > 0:
            tmp = self._past.pop()
            self._future.appendleftentry(tmp)
        while len(self._future) > 0:
            tmp = self._future.popleft()
            if tmp.rev >= rev:
                self._past.appendentry(nothing)
                self._future.clear()
                break
            self._past.appendentry(tmp)

    def __repr__(self):
        return "WindowDict({})".format(repr(dict(self)))

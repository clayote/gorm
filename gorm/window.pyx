from collections import deque, KeysView, ItemsView, ValuesView
        
class WindowDictItemsView(ItemsView):
    def __contains__(self, item):
        cdef int rev, zeroth_past, zeroth_future
        (rev, v) = item
        if len(self._mapping._past) > 0:
            zeroth_past = self._mapping._past[0][0]
            if rev < zeroth_past:
                return False
        elif len(self._mapping._future) > 0:
            zeroth_future = self._mapping._future[0][0]
            if rev < zeroth_future:
                return False
        else:
            return False
        for zeroth_past, mv in self._mapping._past:
            if zeroth_past == rev:
                return mv == v
        for zeroth_past, mv in self._mapping._future:
            if zeroth_past == rev:
                return mv == v
        return False

    def __iter__(self):
        yield from self._mapping._past
        yield from self._mapping._future


class WindowDictValuesView(ValuesView):
    def __contains__(self, value):
        for rev, v in self._mapping._past:
            if v == value:
                return True
        for rev, v in self._mapping._future:
            if v == value:
                return True
        return False

    def __iter__(self):
        for rev, v in self._mapping._past:
            yield v
        for rev, v in self._mapping._future:
            yield v


cdef class QueueEntry:
    cdef public int rev
    cdef public object value
    cdef QueueEntry prev, next

    def __init__(self, int rev, object value, QueueEntry prev=None, QueueEntry next=None):
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

    def __init__(self, QueueEntry head=None, QueueEntry tail=None):
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

    def clear(self):
        self.head = None
        self.tail = None

    def append(self, int rev, object value):
        cdef QueueEntry new_entry = QueueEntry(rev, value, self.tail)
        if self.tail is not None:
            self.tail.next = new_entry
            new_entry.prev = self.tail
        else:
            self.head = new_entry
        self.tail = new_entry

    def appendleft(self, int rev, object value):
        cdef QueueEntry new_entry = QueueEntry(rev, value, None, self.head)
        if self.head is not None:
            self.head.prev = new_entry
            new_entry.next = self.head
        else:
            self.tail = new_entry
        self.head = new_entry

    def _pop_left(self):
        cdef QueueEntry entry = self.head
        if entry is None:
            raise IndexError("pop from empty queue")
        self.head = entry.next
        if self.head is None:
            self.tail = None
        else:
            self.head.prev = None
        return entry

    def popleft(self, int i=0):
        cdef Queue popped
        if i > 0:
            popped = Queue()
            while i > 0:
                popped.append(self.popleft())
                i -= 1
            ret = popped.pop()
            while popped.tail is not None:
                self.appendleft(popped.pop())
            return ret
        elif i < 0:
            return self.pop(i)
        return self._pop_left()

    def _pop(self):
        cdef QueueEntry entry = self.tail
        if entry is None:
            raise IndexError("pop from empty queue")
        self.tail = entry.prev
        if self.tail is None:
            self.head = None
        else:
            self.tail.next = None
        return entry

    def pop(self, int i=-1):
        cdef Queue popped
        if i > -1:
            return self.popleft(i)
        elif i < -1:
            popped = Queue()
            while i < -1:
                popped.append(self.pop())
                i += 1
            ret = popped.pop()
            while popped.tail is not None:
                self.append(popped.pop())
            return ret
        return self._pop()


cdef class WindowDict:
    """A dict that keeps every value that a variable has had over time.
    
    Look up a revision number in this dict and it will give you the effective value as
    of that revision. Keys should always be revision numbers. Once a key is set, all greater
    keys are considered to be in this dict unless the value is ``None``. Keys after that one
    aren't "set" until one's value is non-``None`` again.
    
    Optimized for the cases where you look up the same revision repeatedly, or its neighbors.
    
    """
    cdef public Queue _past, _future

    def seek(self, int rev):
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

    def rev_before(self, int rev):
        """Return the last rev prior to the given one on which the value changed."""
        self.seek(rev)
        if len(self._past) == 0:
            raise KeyError
        return self._past[-1].rev

    def rev_after(self, int rev):
        """Return the next rev after the given one on which the value will change, or None if it never will."""
        self.seek(rev)
        if len(self._future) > 0:
            return self._future[0].rev

    def keys(self):
        return KeysView(self)

    def items(self):
        return WindowDictItemsView(self)

    def values(self):
        return WindowDictValuesView(self)

    def get(self, int rev, default=None):
        try:
            return self[rev]
        except KeyError:
            return default

    def setdefault(self, int rev, default=None):
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

    def clear(self):
        self._past.clear()
        self._future.clear()

    def pop(self, int rev):
        ret = self[rev]
        del self[rev]
        return ret

    def popitem(self, int rev):
        v = self[rev]
        del self[rev]
        return rev, v

    def __init__(self, dict data={}):
        self._past = Queue()
        for rev, v in sorted(data.items()):
            self._past.append(rev, v)
        self._future = Queue()

    def __iter__(self):
        for (rev, v) in self._past:
            yield rev
        for (rev, v) in self._future:
            yield rev

    def __contains__(self, int item):
        cdef int rev
        for rev, v in self._mapping._past:
            if rev == item:
                return True
        for rev, v in self._mapping._future:
            if rev == item:
                return True
        return False

    def __richcmp__(self, WindowDict other, int op):
        if op not in (2, 3):
            raise TypeError
        if op == 2:
            return self._past + self._future == other._past + other._future
        if op == 3:
            return self._past + self._future != other._past + other._future

    def __len__(self):
        return len(self._past) + len(self._future)

    def __getitem__(self, int rev):
        self.seek(rev)
        try:
            return self._past[-1].value
        except IndexError:
            raise KeyError

    def __setitem__(self, int rev, v):
        cdef pastrev
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
        while len(self._past) > 0:
            self._future.appendleft(*self._past.pop())
        while len(self._future) > 0:
            frev, v = self._future.popleft()
            if frev >= rev:
                self._past.append(rev, None)
                self._future.clear()
                return
            self._past.append(frev, v)

    def __repr__(self):
        return "WindowDict({})".format(repr(dict(self)))

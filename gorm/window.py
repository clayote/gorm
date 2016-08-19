from collections import deque, MutableMapping


class WindowDict(MutableMapping):
    """A dict that keeps every value that a variable has had over time.
    
    Look up a revision number in this dict and it will give you the effective value as
    of that revision. Keys should always be revision numbers.
    
    Optimized for the cases where you look up the same revision repeatedly, or its neighbors.
    
    """
    def seek(self, rev):
        """Arrange the caches in the optimal way for looking up the given revision."""
        while self._future and self._future[0][0] < rev:
            self._past.append(self._future.popleft())
        while self._past and self._past[-1][0] > rev:
            self._future.appendleft(self._past.pop())

    def rev_before(self, rev):
        """Return the last rev prior to the given one on which the value changed."""
        self.seek(rev)
        return self._past[-1][0]

    def rev_after(self, rev):
        """Return the next rev after the given one on which the value will change, or None if it never will."""
        self.seek(rev)
        if self._future:
            return self._future[0][0]

    def __init__(self, data={}):
        self._past = deque()
        self._future = deque()
        for (rev, v) in sorted(data.items()):
            if rev <= self._rev:
                self._past.append((rev, v))
            else:
                self._future.append((rev, v))

    def __iter__(self):
        for (rev, v) in self._past:
            yield rev
        for (rev, v) in self._future:
            yield rev

    def __len__(self):
        return len(self._past) + len(self._future)

    def __getitem__(self, rev):
        if not self._past:
            raise KeyError("No history")
        latest = self._past.pop()
        if rev == latest[0]:
            self._past.append(latest)
            return latest[1]
        elif rev > latest[0]:
            if not self._future:
                self._past.append(latest)
                return latest[1]
            nxt = self._future.popleft()
            if rev < nxt[0]:
                self._future.appendleft(nxt)
                self._past.append(latest)
                return latest
            future = self._future
            self._future = deque([nxt])
            while rev < nxt[0]:
                nxt = future.popleft()
                self._future.append(nxt)
            self._future.extend(future)
            return nxt[1]
        else:  # rev < latest[0]
            past = self._past
            less_past = deque([latest])
            while rev < latest[0]:
                try:
                    latest = past.pop()
                except IndexError:
                    self._past = less_past
                    raise KeyError("Revision {} is before the start of history".format(rev))
                less_past.appendleft(latest)
            self._past = past + less_past
            return latest[1]

    def __setitem__(self, rev, v):
        if not self._past:
            self._past.append((rev, v))
        elif rev < self._past[0][0]:
            self._past.appendleft((rev, v))
        elif rev == self._past[0][0]:
            self._past[0] = (rev, v)
        elif rev == self._past[-1][0]:
            self._past[-1] = (rev, v)
        elif rev > self._past[-1][0]:
            if not self._future or rev < self._future[0][0]:
                self._past.append((rev, v))
            elif rev == self._future[0][0]:
                self._future[0][0] = (rev, v)
            elif rev == self._future[-1][0]:
                self._future[-1][0] = (rev, v)
            elif rev > self._future[-1][0]:
                self._future.append((rev, v))
            else:
                self._future.append((rev, v))
                inserted = sorted(self._future)
                self._future = deque(inserted)
        else:
            # I was going to implement my own insertion sort here, but I gather Python already
            # does that, via Timsort. I wonder if there's a way I can give it a hint, so it doesn't
            # have to check for partial ordering? And maybe avoid reconstructing the deque?
            self._past.append((rev, v))
            inserted = sorted(self._past)
            self._past = deque(inserted)
    
    def __delitem__(self, rev):
        name = '_past' if rev <= self._rev else '_future'
        stack = getattr(self, name)
        waste = deque()
        setattr(self, name, waste)
        deleted = False
        while stack:
            (r, v) = stack.popleft()
            if r != rev:
                waste.append((r, v))
            else:
                assert not deleted
                deleted = True
        if not deleted:
            raise KeyError("Rev not present: {}".format(rev))

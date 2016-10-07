from collections import ItemsView, ValuesView, MutableMapping
from .trique import Trique


class WindowDictItemsView(ItemsView):
    def __contains__(self, item):
        for (rev, v) in self._mapping.history:
            if (rev, v) == item:
                return True
        return False

    def __iter__(self):
        return iter(self._mapping.history)


class WindowDictValuesView(ValuesView):
    def __contains__(self, value):
        for (rev, v) in self._mapping.history:
            if v == value:
                return True
        return False

    def __iter__(self):
        for rev, v in self._mapping.history:
            yield v


class WindowDict(MutableMapping):
    """A dict that keeps every value that a variable has had over time.
    
    Look up a revision number in this dict and it will give you the effective value as
    of that revision. Keys should always be revision numbers. Once a key is set, all greater
    keys are considered to be in this dict unless the value is ``None``. Keys after that one
    aren't "set" until one's value is non-``None`` again.
    
    Optimized for the cases where you look up the same revision repeatedly, or its neighbors.
    
    """
    __slots__ = ['history']

    def seek(self, rev):
        """Arrange the caches in the optimal way for looking up the given revision."""
        hoplen = len(self.history) // 2
        if hoplen == 0:
            return
        currev = self.history.middle[0]
        while hoplen > 1:
            if currev == rev:
                return
            elif currev > rev:
                self.history.seek(-hoplen)
            else:  # currev < rev
                self.history.seek(hoplen)
            hoplen //= 2
            currev = self.history.middle[0]
        while currev > rev:
            self.history.seek(-1)
            currev = self.history.middle[0]
        while currev < rev:
            self.history.seek(1)
            currev = self.history.middle[0]

    def rev_before(self, rev):
        """Return the last rev prior to the given one on which the value changed."""
        self.seek(rev)
        return self.history.middle[0]

    def rev_after(self, rev):
        """Return the next rev after the given one on which the value will change, or None if it never will."""
        self.seek(rev)
        self.history.seek(1)
        return self.history.middle[0]

    def items(self):
        return WindowDictItemsView(self)

    def values(self):
        return WindowDictValuesView(self)

    def __init__(self, data={}):
        self.history = Trique(sorted(data.items()))

    def __iter__(self):
        for (rev, v) in self.history:
            yield rev

    def __len__(self):
        return len(self.history)

    def __getitem__(self, rev):
        self.seek(rev)
        ret = self.history.middle[1]
        if ret is None:
            raise KeyError("Set, then deleted")
        return ret

    def __setitem__(self, rev, v):
        if len(self.history) == 0:
            self.history.append((rev, v))
            return
        self.seek(rev)
        if rev == self.history.middle[0]:
            self.history.middle = (rev, v)
        else:
            self.history.insertmiddle((rev, v))
    
    def __delitem__(self, rev):
        self.seek(rev)
        if self.history.middle[0] == rev:
            self.history.middle = (rev, None)
        else:
            self.history.insertmiddle((rev, None))

    def __repr__(self):
        return "WindowDict({})".format(repr(dict(self)))

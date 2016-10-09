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
        self.history.seekrev(rev)

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
        mrev, ret = self.history.middle
        if mrev == 0:
            assert (mrev, ret) == self.history[0]
        if ret is None:
            raise KeyError("Set, then deleted")
        if mrev > rev:
            raise KeyError("No history before {}".format(mrev))
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
        while self.history[-1][0] != rev:
            del self.history[-1]

    def __repr__(self):
        return "WindowDict({})".format(repr(dict(self)))

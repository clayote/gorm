from collections import defaultdict, MutableMapping
from numpy import array, less_equal, greater


def window_left(revs, rev):
    k = frozenset(revs)
    if k not in window_left.memo or rev not in window_left.memo[k]:
        revs = array(tuple(k))
        window_left.memo[k][rev] = revs[less_equal(revs, rev)].max()
    return window_left.memo[k][rev]
window_left.memo = defaultdict(dict)


def window_right(revs, rev):
    k = frozenset(revs)
    if k not in window_right.memo or rev not in window_right.memo[k]:
        revs = array(tuple(k))
        window_right.memo[k][rev] = revs[greater(revs, rev)].min()
    return window_right.memo[k][rev]
window_right.memo = defaultdict(dict)


def window(revs, rev):
    return (
        window_left(revs, rev),
        window_right(revs, rev)
    )


class WindowDict(MutableMapping):
    def __init__(self):
        self._real = {}

    def __iter__(self):
        return iter(self._real)

    def __len__(self):
        return len(self._real)

    def __setitem__(self, k, v):
        self._real[k] = v

    def __delitem__(self, k):
        del self._real[k]

    def __getitem__(self, k):
        if k in self._real:
            return self._real[k]
        try:
            return self._real[
                window_left(self._real.keys(), k)
            ]
        except ValueError:
            raise KeyError(
                "Key {} not set, nor any before it.".format(k)
            )

from collections import defaultdict
from numpy import array, less_equal, greater

window_left_hints = defaultdict(dict)
def window_left(revs, rev):
    k = frozenset(revs)
    revs = array(tuple(revs))
    if k not in window_left_hints or rev not in window_left_hints[k]:
        window_left_hints[k][rev] = revs[less_equal(revs, rev)].max()
    return window_left_hints[k][rev]


window_right_hints = defaultdict(dict)
def window_right(revs, rev):
    k = frozenset(revs)
    revs = array(tuple(revs))
    if k not in window_right_hints or rev not in window_right_hints[k]:
        window_right_hints[k][rev] = revs[greater(revs, rev)].min()
    return window_right_hints[k][rev]


def window(revs, rev):
    return (
        window_left(revs, rev),
        window_right(revs, rev)
    )
# This file is part of gorm, an object relational mapper for versioned graphs.
# Copyright (C) 2014 Zachary Spector.
from collections import MutableMapping
from json import dumps, loads


def enc_tuple(o):
    """Return the object, converted to a form that will preserve the
    distinction between lists and tuples when written to JSON

    """
    if isinstance(o, tuple):
        return ['tuple'] + [enc_tuple(p) for p in o]
    elif isinstance(o, list):
        return ['list'] + [enc_tuple(v) for v in o]
    elif isinstance(o, dict):
        r = {}
        for (k, v) in o.items():
            r[enc_tuple(k)] = enc_tuple(v)
        return r
    else:
        return o


def dec_tuple(o):
    """Take an object previously encoded with ``enc_tuple`` and return it
    with the encoded tuples turned back into actual tuples

    """
    if isinstance(o, dict):
        r = {}
        for (k, v) in o.items():
            r[dec_tuple(k)] = dec_tuple(v)
        return r
    elif isinstance(o, list):
        if o[0] == 'list':
            return list(dec_tuple(p) for p in o[1:])
        else:
            assert(o[0] == 'tuple')
            return tuple(dec_tuple(p) for p in o[1:])
    else:
        return o


json_dump_hints = {}


def json_dump(obj):
    """JSON dumper that distinguishes lists from tuples"""
    k = str(obj)
    if k not in json_dump_hints:
        json_dump_hints[k] = dumps(enc_tuple(obj))
    return json_dump_hints[k]


json_load_hints = {}


def json_load(s):
    """JSON loader that distinguishes lists from tuples"""
    if s is None:
        return None
    if s not in json_load_hints:
        json_load_hints[s] = dec_tuple(loads(s))
    return json_load_hints[s]


def ismutable(v):
    try:
        if isinstance(v, long):
            return False
    except NameError:
        pass
    return not (
        isinstance(v, str) or
        isinstance(v, int) or
        isinstance(v, bool) or
        isinstance(v, float) or
        isinstance(v, tuple)
    )


class JSONWrapper(MutableMapping):
    def __init__(self, outer, outkey):
        self.outer = outer
        self.outkey = outkey

    def __contains__(self, wot):
        return self._get().__contains__(wot)

    def _get(self, k=None):
        if k is None:
            return self.outer._get(self.outkey)
        return self._get()[k]

    def _set(self, v):
        self.outer[self.outkey] = v

    def __iter__(self):
        return iter(self._get())

    def __len__(self):
        return len(self._get())

    def __getitem__(self, k):
        r = self._get()[k]
        if isinstance(r, list):
            return JSONListWrapper(self, k)
        elif ismutable(r):
            return JSONWrapper(self, k)
        else:
            return r

    def __setitem__(self, k, v):
        me = self._get()
        me[k] = v
        self._set(me)

    def __delitem__(self, k):
        me = self._get()
        del me[k]
        self._set(me)

    def __str__(self):
        return self._get().__str__()

    def __repr__(self):
        return self._get().__repr__()

    def __eq__(self, other):
        return self._get() == other

    def __list__(self):
        return list(self._get())

    def __dict__(self):
        return dict(self._get())

    def copy(self):
        return self._get().copy()


class JSONListWrapper(JSONWrapper):
    def append(self, v):
        me = self._get()
        me.append(v)
        self._set(me)

    def insert(self, i, v):
        me = self._get()
        me.insert(i, v)
        self._set(me)

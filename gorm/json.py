import json


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
        json_dump_hints[k] = json.dumps(enc_tuple(obj))
    return json_dump_hints[k]


json_load_hints = {}


def json_load(s):
    """JSON loader that distinguishes lists from tuples"""
    if s is None:
        return None
    if s not in json_load_hints:
        json_load_hints[s] = dec_tuple(json.loads(s))
    return json_load_hints[s]

from operator import itemgetter


class Record(tuple):
    """A named tuple that can express itself in SQL."""
    @property
    def sql_ins(self):
        """An SQL string appropriate for inserting this record into a
        database.

        Doesn't really contain any data, only enough ? to substitute
        it in. Pass this as the first argument to the cursor's
        ``execute`` method, and the tuple itself as the second.

        """
        return "INSERT INTO {} ({}) VALUES ({})".format(
            self.table,
            ", ".join(f for f in self.fields if getattr(self, f) is not None),
            ", ".join("?" for f in self.fields if getattr(self, f) is not None)
        )

    @property
    def sql_del(self):
        """An SQL string appropriate for deleting my record.

        Doesn't really contain any data, only enough ? to substitute
        it in. Pass this as the first argument to the cursor's
        ``execute`` method, and the tuple's ``key`` attribute as the
        second.

        """
        if hasattr(self, 'keynames'):
            kns = self.keynames
        else:
            kns = self.fields
        return "DELETE FROM {} WHERE {};".format(
            self.table,
            " AND ".join(
                "{}={}".format(kn, "?") for kn in kns
            )
        )

    @property
    def key(self):
        """A plain tuple containing the primary key identifying this record."""
        if hasattr(self, 'keynames'):
            kns = self.keynames
        else:
            kns = self.fields
        return tuple(getattr(self, kn) for kn in kns)

    @classmethod
    def _make(cls, iterable):
        """Make a new record from a sequence or iterable"""
        r = tuple.__new__(cls, iterable)
        i = 0
        for field_name in cls.fields:
            setattr(
                r,
                field_name,
                property(
                    itemgetter(i),
                    doc="Alias for field number {}".format(i)
                )
            )
        return r

    def __new__(cls, *args, **kwargs):
        """Return a new Record with the values given."""
        if len(args) == len(cls.fields):
            i = 0
            while i < len(args):
                if cls.fields[i] not in kwargs:
                    kwargs[cls.fields[i]] = args[i]
            i += 1
        elif len(args) > 0:
            raise ValueError("Wrong number of values")
        values = [
            kwargs[fieldn] if fieldn in kwargs else None
            for fieldn in cls.fields
        ]
        return cls._make(*values)

    def __getnewargs__(self):
        """Return self as a plain tuple. Used by copy and pickle."""
        return tuple(self)

    def __repr__(self):
        return "{}({})".format(
            self.table,
            ", ".join(
                "{}={}".format(field, getattr(self, field))
                for field in self.fields
            )
        )

    def _replace(self, **kwds):
        """Return a new Record replacing specified fields with new values"""
        result = self._make(map(kwds.pop, self.fields, self))
        if kwds:
            raise ValueError("Got unexpected field names:".format(kwds.keys()))
        return result


class NodeRecord(Record):
    table = 'nodes'
    fields = [
        'graph',
        'node',
        'branch',
        'rev',
        'exists'
    ]
    keynames = [
        'graph',
        'node',
        'branch',
        'rev'
    ]

class NodeValRecord(Record):
    table = 'node_val'
    fields = [
        'graph',
        'node',
        'key',
        'branch',
        'rev',
        'value',
        'type'
    ]
    keynames = [
        'graph',
        'node',
        'key',
        'branch',
        'rev'
    ]


class EdgeRecord(Record):
    table = 'edges'
    fields = [
        'graph',
        'nodeA',
        'nodeB',
        'idx',
        'branch',
        'rev',
        'exists'
    ]
    keynames = [
        'graph',
        'nodeA',
        'nodeB',
        'idx',
        'key',
        'branch',
        'rev'
    ]


class EdgeValRecord(Record):
    table = 'edge_val'
    fields = [
        'graph',
        'nodeA',
        'nodeB',
        'idx',
        'key',
        'branch',
        'rev',
        'value',
        'type'
    ]
    keynames = [
        'graph',
        'nodeA',
        'nodeB',
        'idx',
        'key',
        'branch',
        'rev'
    ]


class GraphRecord(Record):
    table = 'graph'
    fields = [
        'graph',
        'type'
    ]
    keynames = [
        'graph'
    ]


class GraphValRecord(Record):
    table = 'graph_val'
    fields = [
        'graph',
        'key',
        'branch',
        'rev',
        'value'
    ]
    keynames = [
        'graph',
        'key',
        'branch',
        'rev'
    ]


class BranchRecord(Record):
    table = 'branch'
    fields = [
        'branch',
        'parent',
        'parent_rev'
    ]
    keynames = [
        'branch'
    ]

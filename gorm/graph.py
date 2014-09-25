import networkx
import json
from networkx.exception import NetworkXError
from collections import MutableMapping
from sqlite3 import IntegrityError
from gorm.sql import window


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


class GraphMapping(MutableMapping):
    """Mapping for graph attributes"""
    def __init__(self, graph):
        """Initialize private dict and store pointers to the graph and ORM"""
        self.graph = graph
        self.gorm = graph.gorm

    def __iter__(self):
        """Iterate over the keys that are set"""
        seen = set()
        for (branch, rev) in self.gorm._active_branches():
            data = self.gorm.cursor.execute(
                self.gorm.sql('graph_val_keys_set'),
                (
                    self.graph._name,
                    branch,
                    rev
                )
            ).fetchall()
            if len(data) == 0:
                continue
            for (k, v) in data:
                if k not in seen and v is not None:
                    yield json_load(k)
                seen.add(k)

    def __contains__(self, k):
        """Do I have a value for this key right now?"""
        key = json_dump(k)
        for (branch, rev) in self.gorm._active_branches():
            data = self.gorm.cursor.execute(
                self.gorm.sql('graph_val_key_set'),
                (
                    self.graph._name,
                    key,
                    branch,
                    rev
                )
            ).fetchall()
            if len(data) == 0:
                continue
            assert(len(data) == 1)
            return data[0][0] is not None

    def __len__(self):
        """Number of set keys"""
        n = 0
        for k in iter(self):
            n += 1
        return n

    def __getitem__(self, key):
        """If key is 'graph', return myself as a dict, else get the present
        value of the key and return that

        """
        if key == 'graph':
            return dict(self)
        for (branch, rev) in self.gorm._active_branches():
            results = self.gorm.cursor.execute(
                self.gorm.sql('graph_val_present_value'),
                (
                    self.graph._name,
                    json_dump(key),
                    branch,
                    rev
                )
            ).fetchall()
            if len(results) == 0:
                continue
            elif len(results) > 1:
                raise ValueError("Silly data in graph_val table")
            elif results[0][0] is None:
                raise KeyError("Key is not set now")
            else:
                return json_load(results[0][0])
        raise KeyError("key is not set, ever")

    def __setitem__(self, key, value):
        """Set key=value at the present branch and revision"""
        branch = self.gorm.branch
        rev = self.gorm.rev
        k = json_dump(key)
        v = json_dump(value)
        try:
            self.gorm.cursor.execute(
                self.gorm.sql('graph_val_ins'),
                (
                    self.graph._name,
                    k,
                    branch,
                    rev,
                    v
                )
            )
        except IntegrityError:
            self.gorm.cursor.execute(
                self.gorm.sql('graph_val_upd'),
                (
                    v,
                    self.graph._name,
                    k,
                    branch,
                    rev
                )
            )

    def __delitem__(self, key):
        """Indicate that the key has no value at this time"""
        branch = self.gorm.branch
        rev = self.gorm.rev
        k = json_dump(key)
        try:
            self.gorm.cursor.execute(
                self.gorm.sql('graph_val_insdel'),
                (
                    self.graph._name,
                    k,
                    branch,
                    rev,
                    None
                )
            )
        except IntegrityError:
            self.gorm.cursor.execute(
                self.gorm.sql('graph_val_upddel'),
                (
                    None,
                    self.graph._name,
                    k,
                    branch,
                    rev
                )
            )

    def clear(self):
        """Delete everything"""
        for k in iter(self):
            del self[k]

    def __repr__(self):
        """Looks like a dictionary."""
        return repr(dict(self))

    def window(self, branch, revfrom, revto):
        """Return a dict of lists of the values assigned to my keys on each
        revision from ``revfrom`` to ``revto`` in branch
        ``branch``.

        """
        return window(
            "graph_val",
            ("graph",),
            (self.graph._name,),
            branch,
            revfrom,
            revto
        )

    def future(self, revs):
        """Return a dict of lists of the values assigned to my keys in the
        next ``revs`` revisions.

        """
        rev = self.gorm.rev
        return self.window(self.gorm.branch, rev, rev + revs)

    def past(self, revs):
        """Return a dict of lists of the values assigned to my keys in the
        previous ``revs`` revisions.

        """
        rev = self.gorm.rev
        return self.window(self.gorm.branch, rev - revs, rev)

    def update(self, other):
        """Version of ``update`` that doesn't clobber the database so much"""
        for (k, v) in other.items():
            if (
                    k not in self or
                    self[k] != v
            ):
                self[k] = v


class GraphNodeMapping(GraphMapping):
    """Mapping for nodes in a graph"""
    def __init__(self, graph):
        self.graph = graph
        self.gorm = graph.gorm

    def __iter__(self):
        """Iterate over the names of the nodes"""
        return self.gorm._iternodes(self.graph.name)

    def __contains__(self, node):
        """Return whether the node exists presently"""
        return self.gorm._node_exists(self.graph.name, node)

    def __len__(self):
        """How many nodes exist right now?"""
        return self.gorm._countnodes(self.graph.name)

    def __getitem__(self, node):
        """If the node exists at present, return it, else throw KeyError"""
        r = self.Node(self.graph, node)
        if not r.exists:
            raise KeyError("Node doesn't exist")
        return r

    def __setitem__(self, node, dikt):
        """Only accept dict-like values for assignment. These are taken to be
        dicts of node attributes, and so, a new GraphNodeMapping.Node
        is made with them, perhaps clearing out the one already there.

        """
        n = self.Node(self.graph, node)
        n.clear()
        n.exists = True
        n.update(dikt)

    def __delitem__(self, node):
        """Indicate that the given node no longer exists"""
        n = self.Node(self.graph, node)
        if not n.exists:
            raise KeyError("No such node")
        n.clear()

    def __eq__(self, other):
        """Compare values cast into dicts.

        As I serve the custom Node class, rather than dicts like
        networkx normally would, the normal comparison operation would
        not let you compare my nodes with regular networkx
        nodes-that-are-dicts. So I cast my nodes into dicts for this
        purpose, and cast the other argument's nodes the same way, in
        case it is a gorm graph.

        """
        if not hasattr(other, 'keys'):
            return False
        if set(self.keys()) != set(other.keys()):
            return False
        for k in self.keys():
            if dict(self[k]) != dict(other[k]):
                return False
        return True

    class Node(GraphMapping):
        """Mapping for node attributes"""

        @property
        def exists(self):
            return self.gorm._node_exists(self.graph.name, self.node)

        @exists.setter
        def exists(self, v):
            if not isinstance(v, bool):
                raise TypeError("Existence is boolean")
            branch = self.gorm.branch
            rev = self.gorm.rev
            try:
                self.gorm.cursor.execute(
                    self.gorm.sql('exist_node_ins'),
                    (
                        self.graph._name,
                        self._node,
                        branch,
                        rev,
                        v
                    )
                )
            except IntegrityError:
                self.gorm.cursor.execute(
                    self.gorm.sql('exist_node_upd'),
                    (
                        v,
                        self.graph._name,
                        self._node,
                        branch,
                        rev
                    )
                )

        @property
        def node(self):
            return json_load(self._node)

        def __init__(self, graph, node):
            """Store name and graph"""
            self.graph = graph
            self.gorm = graph.gorm
            self._node = json_dump(node)

        def __iter__(self):
            """Iterate over those keys that are set at the moment

            """
            seen = set()
            for (branch, rev) in self.gorm._active_branches():
                data = self.gorm.cursor.execute(
                    self.gorm.sql('node_val_keys'),
                    (
                        self.graph._name,
                        self._node,
                        branch,
                        rev
                    )
                ).fetchall()
                for (key,) in data:
                    k = json_load(key)
                    if k not in seen:
                        yield k
                    seen.add(k)

        def __contains__(self, k):
            """Does the key have a value at the moment?"""
            key = json_dump(k)
            for (branch, rev) in self.gorm._active_branches():
                data = self.gorm.cursor.execute(
                    self.gorm.sql('node_val_vals'),
                    (
                        self.graph._name,
                        self._node,
                        key,
                        branch,
                        rev
                    )
                ).fetchall()
                if len(data) == 0:
                    continue
                elif len(data) > 1:
                    raise ValueError("Silly data in node_val table")
                else:
                    return data[0][0] is not None
            return False

        def __getitem__(self, key):
            """Get the value of the key at the present branch and rev"""
            k = json_dump(key)
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    self.gorm.sql('node_val_get_val'),
                    (
                        self.graph._name,
                        self._node,
                        k,
                        branch,
                        rev
                    )
                )
                data = self.gorm.cursor.fetchall()
                if len(data) == 0:
                    continue
                elif len(data) > 1:
                    raise ValueError("Silly data in node_val table")
                else:
                    return json_load(data[0][0])
            raise KeyError("Key not set")

        def __setitem__(self, key, value):
            """Set key=value at the present branch and rev. Overwrite if
            necessary.

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            k = json_dump(key)
            v = json_dump(value)
            try:
                self.gorm.cursor.execute(
                    self.gorm.sql('node_val_set_val_ins'),
                    (
                        self.graph._name,
                        self._node,
                        k,
                        branch,
                        rev,
                        v
                    )
                )
            except IntegrityError:
                self.gorm.cursor.execute(
                    self.gorm.sql('node_val_set_val_upd'),
                    (
                        v,
                        self.graph._name,
                        self._node,
                        k,
                        branch,
                        rev
                    )
                )

        def __delitem__(self, key):
            """Set the key's value to NULL, indicating it should be ignored
            now and in future revs

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            k = json_dump(key)
            try:
                self.gorm.cursor.execute(
                    self.gorm.sql('node_val_del_key_ins'),
                    (self.graph._name, self._node, k, branch, rev)
                )
            except IntegrityError:
                self.gorm.cursor.execute(
                    self.gorm.sql('node_val_del_key_upd'),
                    (
                        self.graph._name,
                        self._node,
                        k,
                        branch,
                        rev
                    )
                )

        def clear(self):
            """Delete everything and stop existing"""
            for k in self:
                del self[k]
            self.exists = False

        def changes(self):
            """Return a dictionary describing changes in my stats between the
            current tick and the previous.

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            # special case when the tick is right at the beginning of a branch
            self.gorm.cursor.execute(
                self.gorm.sql('parparrev'),
                (branch,)
            )
            (parent, parent_rev) = self.engine.cursor.fetchone()
            before_branch = parent if parent_rev == rev else branch
            return self.compare(before_branch, rev-1, branch, rev)

        def compare(self, before_branch, before_rev, after_branch, after_rev):
            """Return a dict of pairs of values assigned to my keys.

            The first element of each pair is the value at
            ``(before_branch, before_rev)``. The second is the value
            at ``(after_branch, after_rev)``.

            """
            self.gorm.cursor.execute(
                self.gorm.sql('node_val_compare'),
                (
                    self.graph._name,
                    self._node,
                    before_branch,
                    before_rev,
                    self.graph._name,
                    self._node,
                    after_branch,
                    after_rev
                )
            )
            r = {}
            for (key, val0, val1) in self.gorm.cursor.fetchall():
                r[json_load(key)] = (json_load(val0), json_load(val1))
            return r

        def window(self, branch, revfrom, revto):
            """Return a dict of lists of the values assigned to my keys each
            revision from ``revfrom`` to ``revto`` in the branch
            ``branch``.

            """
            return window(
                "node_val",
                ("graph", "node"),
                (json_dump(self.graph.name), self._node),
                branch,
                revfrom,
                revto
            )

        def future(self, revs):
            """Return a dict of lists of the values assigned to my keys in each of
            the next ``revs`` revisions.

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            return self.window(branch, rev, rev + revs)

        def past(self, revs):
            """Return a dict of lists of the values assigned to each of my keys in
            each of the previous ``revs`` revisions.

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            return self.window(branch, rev - revs, rev)


class GraphEdgeMapping(GraphMapping):
    """Provides an adjacency mapping and possibly a predecessor mapping
    for a graph.

    """
    def __init__(self, graph):
        """Store the graph"""
        self.graph = graph
        self.gorm = graph.gorm

    def __len__(self):
        """How many nodes do I have at the moment?"""
        return self.gorm._countnodes(self.graph.name)

    def __eq__(self, other):
        """Compare dictified versions of the edge mappings within me.

        As I serve custom Predecessor or Successor classes, which
        themselves serve the custom Edge class, I wouldn't normally be
        comparable to a networkx adjacency dictionary. Converting
        myself and the other argument to dicts allows the comparison
        to work anyway.

        """
        if not hasattr(other, 'keys'):
            return False
        myks = set(self.keys())
        if myks != set(other.keys()):
            return False
        for k in myks:
            if dict(self[k]) != dict(other[k]):
                return False
        return True

    def __iter__(self):
        for o in self.gorm._iternodes(self.graph):
            yield o

    class Edge(GraphMapping):
        """Mapping for edge attributes"""
        def __init__(self, graph, nodeA, nodeB, idx=0):
            """Store the graph, the names of the nodes, and the index.

            For non-multigraphs the index is always 0.

            """
            self.graph = graph
            self.gorm = graph.gorm
            self._nodeA = json_dump(nodeA)
            self._nodeB = json_dump(nodeB)
            self.idx = idx

        @property
        def nodeA(self):
            return json_load(self._nodeA)

        @property
        def nodeB(self):
            return json_load(self._nodeB)

        @property
        def exists(self):
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    self.gorm.sql('edge_extant'),
                    (
                        json_dump(self.graph.name),
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        branch,
                        rev
                    )
                )
                data = self.gorm.cursor.fetchall()
                if len(data) == 0:
                    continue
                elif len(data) > 1:
                    raise ValueError("Silly data in edges table")
                else:
                    return bool(data.pop()[0])
            return False  # also, how did I get here

        @exists.setter
        def exists(self, v):
            if not isinstance(v, bool):
                raise TypeError("Existence is boolean")
            branch = self.gorm.branch
            rev = self.gorm.rev
            try:
                self.gorm.cursor.execute(
                    self.gorm.sql('edge_exist_ins'),
                    (
                        self.graph._name,
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        branch,
                        rev,
                        v
                    )
                )
            except IntegrityError:
                self.gorm.cursor.execute(
                    self.gorm.sql('edge_exist_upd'),
                    (
                        v,
                        self.graph._name,
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        branch,
                        rev
                    )
                )

        def __iter__(self):
            """Yield those keys that have a value"""
            seen = set()
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    self.gorm.sql('edge_val_keys'),
                    (
                        self.graph._name,
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        branch,
                        rev
                    )
                )
                for (key,) in self.gorm.cursor.fetchall():
                    k = json_load(key)
                    if k not in seen:
                        yield k
                    seen.add(k)

        def __contains__(self, k):
            """Does this key have a value at the moment?"""
            key = json_dump(k)
            for (branch, rev) in self.gorm._active_branches():
                data = self.gorm.cursor.execute(
                    self.gorm.sql('edge_val_contains'),
                    (
                        self.graph._name,
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        key,
                        branch,
                        rev
                    )
                ).fetchall()
                if len(data) == 0:
                    continue
                assert(len(data) == 1)
                return data[0][0] is not None

        def __getitem__(self, key):
            """Return the present value of the key, or raise KeyError if it's
            unset

            """
            k = json_dump(key)
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    self.gorm.sql('edge_val_get'),
                    (
                        self.graph._name,
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        k,
                        branch,
                        rev
                    )
                )
                data = self.gorm.cursor.fetchall()
                if len(data) == 0:
                    continue
                elif len(data) > 1:
                    raise ValueError("Silly data in edge_val table")
                else:
                    return json_load(data[0][0])
            raise KeyError('key never set')

        def __setitem__(self, key, value):
            """Set a database record to say that key=value at the present branch
            and revision

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            k = json_dump(key)
            v = json_dump(value)
            try:
                self.gorm.cursor.execute(
                    self.gorm.sql('edge_val_set_ins'),
                    (
                        self.graph._name,
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        k,
                        branch,
                        rev,
                        v
                    )
                )
            except IntegrityError:
                self.gorm.cursor.execute(
                    self.gorm.sql('edge_val_set_upd'),
                    (
                        v,
                        self.graph._name,
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        k,
                        branch,
                        rev
                    )
                )

        def __delitem__(self, key):
            """Set the key's value to NULL, such that it is not yielded by
            ``__iter__``

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            k = json_dump(key)
            try:
                self.gorm.cursor.execute(
                    self.gorm.sql('edge_val_del_ins'),
                    (
                        self.graph._name,
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        k,
                        branch,
                        rev,
                        None
                    )
                )
            except IntegrityError:
                self.gorm.cursor.execute(
                    self.gorm.sql('edge_val_del_upd'),
                    (
                        None,
                        self.graph._name,
                        self._nodeA,
                        self._nodeB,
                        self.idx,
                        k,
                        branch,
                        rev
                    )
                )

        def clear(self):
            """Delete everything, and declare that I don't exist"""
            for k in self:
                del self[k]
            self.exists = False

        def compare(self, branch_before, rev_before, branch_after, rev_after):
            self.gorm.cursor.execute(
                self.gorm.sql('edge_val_compare'),
                (
                    self.graph._name,
                    self._nodeA,
                    self._nodeB,
                    self.idx,
                    branch_before,
                    rev_before,
                    self.graph._name,
                    self._nodeA,
                    self._nodeB,
                    self.idx,
                    branch_after,
                    rev_after
                )
            )

        def changes(self):
            """Return a dictionary describing changes in my stats between the
            current tick and the previous.

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                self.gorm.sql('parparrev'),
                (branch,)
            )
            (parent, parent_rev) = self.gorm.cursor.fetchone()
            before_branch = parent if rev == parent_rev else branch
            return self.compare(before_branch, rev-1, branch, rev)

        def window(self, branch, revfrom, revto):
            """Return a dict of lists of the values assigned to my keys each
            revision from ``revfrom`` to ``revto`` in the branch ``branch``.

            """
            return window(
                "edge_vals",
                ("graph", "nodeA", "nodeB", "idx"),
                (self.graph._name, self._nodeA, self._nodeB, self.idx),
                branch,
                revfrom,
                revto
            )

        def future(self, revs):
            """Return a dict of lists of the values assigned to my keys in each of
            the next ``revs`` revisions.

            """
            rev = self.gorm.rev
            return self.window(self.gorm.branch, rev, rev + revs)

        def past(self, revs):
            """Return a dict of lists of the values assigned to each of my keys in
            each of the previous ``revs`` revisions.

            """
            rev = self.gorm.rev
            return self.window(self.gorm.branch, rev - revs, rev)


class GraphSuccessorsMapping(GraphEdgeMapping):
    """Mapping for Successors (itself a MutableMapping)"""
    def __getitem__(self, nodeA):
        """If the node exists, return a Successors instance for it"""
        if not self.gorm._node_exists(self.graph.name, nodeA):
            raise KeyError("No such node")
        return self.Successors(self, nodeA)

    def __setitem__(self, nodeA, val):
        """Wipe out any edges presently emanating from nodeA and replace them
        with those described by val

        """
        sucs = self.Successors(self, nodeA)
        sucs.clear()
        sucs.update(val)

    def __delitem__(self, nodeA):
        """Wipe out edges emanating from nodeA"""
        self.Successors(self, nodeA).clear()

    def __iter__(self):
        """Iterate over nodes that have at least one outgoing edge"""
        seen = set()
        for (branch, rev) in self.gorm._active_branches():
            data = self.gorm.cursor.execute(
                self.gorm.sql('edgeiter'),
                (self.graph._name, branch, rev)
            ).fetchall()
            for (a, extant) in data:
                nodeA = json_load(a)
                if nodeA not in seen and extant:
                    yield nodeA
                seen.add(nodeA)

    def __contains__(self, nodeA):
        """Does this node exist, and does it have at least one outgoing
        edge?

        """
        a = json_dump(nodeA)
        for (branch, rev) in self.gorm._active_branches():
            r = self.gorm.cursor.execute(
                self.gorm.sql('nodeBiter'),
                (self.graph._name, a, branch, rev)
            ).fetchone()
            if r is not None:
                return bool(r[1])
        return False

    class Successors(GraphEdgeMapping):
        @property
        def nodeA(self):
            return json_load(self._nodeA)

        def _getsub(self, nodeB):
            """Return what I map to"""
            return self.Edge(self.graph, self.nodeA, nodeB)

        def __init__(self, container, nodeA):
            """Store container and node"""
            self.container = container
            self.graph = container.graph
            self.gorm = self.graph.gorm
            self._nodeA = json_dump(nodeA)

        def __iter__(self):
            """Iterate over node IDs that have an edge with my nodeA"""
            seen = set()
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    self.gorm.sql('nodeBiter'),
                    (
                        self.graph._name,
                        self._nodeA,
                        branch,
                        rev
                    )
                )
                for row in self.gorm.cursor.fetchall():
                    nodeB = json_load(row[0])
                    extant = bool(row[1])
                    if nodeB not in seen and extant:
                        yield nodeB
                    seen.add(nodeB)

        def __contains__(self, nodeB):
            """Is there an edge leading to ``nodeB`` at the moment?"""
            b = json_dump(nodeB)
            for (branch, rev) in self.gorm._active_branches():
                data = self.gorm.cursor.execute(
                    self.gorm.sql('nodeBiter'),
                    (
                        self.graph._name,
                        self._nodeA,
                        b,
                        branch,
                        rev
                    )
                ).fetchall()
                r = [bool(row[1]) for row in data]
                if len(r) > 0:
                    return any(r)
            return False

        def __len__(self):
            """How many nodes touch an edge shared with my nodeA?"""
            n = 0
            for nodeB in iter(self):
                n += 1
            return n

        def __getitem__(self, nodeB):
            """Get the edge between my nodeA and the given node"""
            r = self._getsub(nodeB)
            if not r.exists:
                raise KeyError("Edge doesn't exist")
            return r

        def __setitem__(self, nodeB, value):
            """Set the edge between my nodeA and the given nodeB to the given
            value, a mapping.

            """
            e = self.Edge(self.graph, self.nodeA, nodeB)
            e.clear()
            e.exists = True
            e.update(value)

        def __delitem__(self, nodeB):
            """Remove the edge between my nodeA and the given nodeB"""
            e = self.Edge(self.graph, self.nodeA, nodeB)
            if not e.exists:
                raise KeyError("No such edge")
            e.clear()

        def clear(self):
            """Delete every edge with origin at my nodeA"""
            for nodeB in self:
                del self[nodeB]


class DiGraphPredecessorsMapping(GraphEdgeMapping):
    """Mapping for Predecessors instances, which map to Edges that end at
    the nodeB provided to this

    """
    def __getitem__(self, nodeB):
        """Return a Predecessors instance for edges ending at the given
        node

        """
        if not self.gorm._node_exists(self.graph.name, nodeB):
            raise KeyError("No such node")
        return self.Predecessors(self, nodeB)

    def __setitem__(self, nodeB, val):
        """Interpret ``val`` as a mapping of edges that end at ``nodeB``"""
        preds = self.Predecessors(self, nodeB)
        preds.clear()
        preds.update(val)

    def __delitem__(self, nodeB):
        """Delete all edges ending at ``nodeB``"""
        self.Predecessors(self, nodeB).clear()

    def __iter__(self):
        """Iterate over nodes with at least one edge leading to them"""
        seen = set()
        for (branch, rev) in self.gorm._active_branches():
            data = self.gorm.cursor.execute(
                self.gorm.sql('nodeBiter'),
                (self.graph._name, branch, rev)
            ).fetchall()
            for (nodeB, extant) in data:
                if nodeB not in seen and extant:
                    yield json_load(nodeB)
                seen.add(nodeB)

    def __contains__(self, nodeB):
        """Does the node exist and have at least one edge leading to it?"""
        b = json_dump(nodeB)
        for (branch, rev) in self.gorm._active_branches():
            r = self.gorm.cursor.execute(
                self.gorm.sql('nodeBiter'),
                (self.graph._name, b, branch, rev)
            ).fetchone()
            if r is not None:
                return bool(r[1])
        return False

    class Predecessors(GraphEdgeMapping):
        """Mapping of Edges that end at a particular node"""
        def _getsub(self, nodeA):
            """Get the edge ending at my nodeB, starting at the given node"""
            return self.Edge(self.graph, nodeA, self.nodeB)

        def __init__(self, container, nodeB):
            """Store container and node ID"""
            self.container = container
            self.graph = container.graph
            self.gorm = self.graph.gorm
            self._nodeB = json_dump(nodeB)

        @property
        def nodeB(self):
            return json_load(self._nodeB)

        def __iter__(self):
            """Iterate over the edges that exist at the present (branch, rev)

            """
            seen = set()
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    self.gorm.sql('nodeAiter'),
                    (
                        self.graph._name,
                        self._nodeB,
                        branch,
                        rev
                    )
                )
                for row in self.gorm.cursor.fetchall():
                    nodeA = row[0]
                    extant = bool(row[1])
                    if nodeA not in seen and extant:
                        yield json_load(nodeA)
                    seen.add(nodeA)

        def __contains__(self, nodeA):
            """Is there an edge from ``nodeA`` at the moment?"""
            a = json_dump(nodeA)
            for (branch, rev) in self.gorm._active_branches():
                data = self.gorm.cursor.execute(
                    self.gorm.sql('edge_exists'),
                    (
                        self.graph._name,
                        a,
                        self._nodeB,
                        branch,
                        rev
                    )
                ).fetchall()
                r = [bool(row[0]) for row in data]
                if len(r) > 0:
                    return any(r)
            return False

        def __len__(self):
            """How many edges exist at this rev of this branch?"""
            n = 0
            for nodeA in iter(self):
                n += 1
            return n

        def __getitem__(self, nodeA):
            """Get the edge from the given node to mine"""
            r = self._getsub(nodeA)
            if not r.exists:
                raise KeyError("Edge doesn't exist")
            return r

        def __setitem__(self, nodeA, value):
            """Use ``value`` as a mapping of edge attributes, set an edge from the
            given node to mine.

            """
            e = self._getsub(nodeA)
            e.clear()
            e.exists = True
            e.update(value)

        def __delitem__(self, nodeA):
            """Unset the existence of the edge from the given node to mine"""
            e = self._getsub(nodeA)
            if not e.exists:
                raise KeyError("No such edge")
            e.clear()


class MultiEdges(GraphEdgeMapping):
    """Mapping of Edges between two nodes"""
    def __init__(self, graph, nodeA, nodeB):
        """Store graph and node IDs"""
        self.graph = graph
        self.gorm = graph.gorm
        self._nodeA = json_dump(nodeA)
        self._nodeB = json_dump(nodeB)

    @property
    def nodeA(self):
        return json_load(self._nodeA)

    @property
    def nodeB(self):
        return json_load(self._nodeB)

    def __iter__(self):
        seen = set()
        for (branch, rev) in self.gorm._active_branches():
            data = self.gorm.cursor.execute(
                self.gorm.sql('multi_edges_iter'),
                (
                    self.graph._name,
                    self._nodeA,
                    self._nodeB,
                    branch,
                    rev
                )
            )
            for (idx, extant) in data:
                if idx not in seen:
                    if extant:
                        yield idx
                seen.add(idx)

    def __len__(self):
        """How many edges currently connect my two nodes?"""
        n = 0
        for idx in iter(self):
            n += 1
        return n

    def __getitem__(self, idx):
        """Get an Edge with a particular index, if it exists at the present
        (branch, rev)

        """
        r = self.Edge(self.graph, self.nodeA, self.nodeB, idx)
        if not r.exists:
            raise KeyError("No edge at that index")
        return r

    def __setitem__(self, idx, val):
        """Create an Edge at a given index from a mapping. Delete the existing
        Edge first, if necessary.

        """
        e = self.Edge(self.graph, self.nodeA, self.nodeB, idx)
        e.clear()
        e.exists = True
        e.update(val)

    def __delitem__(self, idx):
        """Delete the edge at a particular index"""
        e = self.Edge(self.graph, self.nodeA, self.nodeB, idx)
        if not e.exists:
            raise KeyError("No edge at that index")
        e.clear()

    def clear(self):
        """Delete all edges between these nodes"""
        for idx in self:
            del self[idx]


class MultiGraphSuccessorsMapping(GraphSuccessorsMapping):
    """Mapping of Successors that map to MultiEdges"""
    def __getitem__(self, nodeA):
        """If the node exists, return its Successors"""
        if not self.gorm._node_exists(self.graph.name, nodeA):
            raise KeyError("No such node")
        return self.Successors(self, nodeA)

    def __setitem__(self, nodeA, val):
        """Interpret ``val`` as a mapping of successors, and turn it into a
        proper Successors object for storage

        """
        r = self.Successors(self, nodeA)
        r.clear()
        r.update(val)

    def __delitem__(self, nodeA):
        """Disconnect this node from everything"""
        self.Successors(self, nodeA).clear()

    class Successors(GraphSuccessorsMapping.Successors):
        """Edges succeeding a given node in a multigraph"""
        def _getsub(self, nodeB):
            """Get MultiEdges"""
            return MultiEdges(self.graph, self.nodeA, nodeB)

        def __getitem__(self, nodeB):
            """If ``nodeB`` exists, return the edges to it"""
            r = self._getsub(nodeB)
            if len(r) == 0:
                raise KeyError("No edge between these nodes")
            return r

        def __setitem__(self, nodeB, val):
            """Interpret ``val`` as a dictionary of edge attributes for edges
            between my ``nodeA`` and the given ``nodeB``

            """
            self._getsub(nodeB).update(val)

        def __delitem__(self, nodeB):
            """Delete all edges between my ``nodeA`` and the given ``nodeB``"""
            self._getsub(nodeB).clear()


class MultiDiGraphPredecessorsMapping(DiGraphPredecessorsMapping):
    """Version of DiGraphPredecessorsMapping for multigraphs"""
    class Predecessors(DiGraphPredecessorsMapping.Predecessors):
        """Predecessor edges from a given node"""
        def _getsub(self, nodeA):
            """Get MultiEdges"""
            return MultiEdges(self.graph, nodeA, self.nodeB)


class GormGraph(object):
    """Class giving the gorm graphs those methods they share in
    common.

    """
    def _init_atts(self, gorm, name):
        """Initialize the mappings that are the same for all gorm graphs"""
        self._name = json_dump(name)
        self.gorm = gorm
        self.graph = GraphMapping(self)
        self.window = self.graph.window
        self.future = self.graph.future
        self.past = self.graph.past
        self.node = GraphNodeMapping(self)

    @property
    def name(self):
        return json_load(self._name)

    @name.setter
    def name(self, v):
        raise TypeError("gorm graphs can't be renamed")

    def _and_previous(self):
        """Return a 4-tuple that will usually be (current branch, current
        revision - 1, current branch, current revision), unless
        current revision - 1 is before the start of the current
        branch, in which case the first element will be the parent
        branch.

        """
        branch = self.gorm.branch
        rev = self.gorm.rev
        self.gorm.cursor.execute(
            "SELECT parent, parent_rev FROM branches WHERE branch=?;",
            (branch,)
        )
        (parent, parent_rev) = self.engine.cursor.fetchone()
        before_branch = parent if parent_rev == rev else branch
        return (before_branch, rev-1, branch, rev)

    def compare_nodes(
            self,
            before_branch,
            before_rev,
            after_branch,
            after_rev
    ):
        """Return a dict describing changes to my nodes between the given revisions.

        """
        r = {}
        for node in self.node.values():
            r[node.name] = node.compare(
                before_branch,
                before_rev,
                after_branch,
                after_rev
            )
        return r

    def node_changes(self):
        """Return a dict describing changes to my nodes between the present
        revision and the previous.

        """
        return self.compare_nodes(*self._and_previous())

    def compare_edges(
            self,
            before_branch,
            before_rev,
            after_branch,
            after_rev
    ):
        """Return a dict describing changes to my edges between the given revisions.

        """
        r = {}
        for nodeA in self.edge:
            if nodeA not in r:
                r[nodeA] = {}
            for nodeB in self.edge[nodeA]:
                maybe_edge = self.edge[nodeA][nodeB]
                if isinstance(maybe_edge, GraphEdgeMapping.Edge):
                    r[nodeA][nodeB] = maybe_edge.compare(
                        before_branch,
                        before_rev,
                        after_branch,
                        after_rev
                    )
                else:
                    if nodeB not in r[nodeA]:
                        r[nodeA][nodeB] = {}
                    for idx in maybe_edge:
                        r[nodeA][nodeB][idx] = self.edge[
                            nodeA
                        ][
                            nodeB
                        ][
                            idx
                        ].compare(
                            before_branch,
                            before_rev,
                            after_branch,
                            after_rev
                        )
        return r

    def edge_changes(self):
        """Return a dict describing changes to my edges between the present
        revision and the previous.

        """
        return self.compare_edges(*self._and_previous())

    def compare(self, branch_before, rev_before, branch_after, rev_after):
        """Return a dict describing changes to my attributes between the given
        revisions.

        """
        self.gorm.cursor.execute(
            self.gorm.sql('graph_compare'),
            (
                self._name,
                branch_before,
                rev_before,
                self._name,
                branch_after,
                rev_after
            )
        )
        r = {}
        for (key, val0, val1) in self.gorm.cursor.fetchall():
            if val0 != val1:
                r[key] = (json_load(val0), json_load(val1))
        return r

    def changes(self):
        """Return a dict describing changes to my attributes between this
        revision and the previous.

        """
        return self.compare(*self._and_previous())

    def clear(self):
        """Remove all nodes and edges from the graph.

        Unlike the regular networkx implementation, this does *not*
        remove the graph's name. But all the other graph, node, and
        edge attributes go away.

        """
        self.adj.clear()
        self.node.clear()
        self.graph.clear()


class Graph(networkx.Graph, GormGraph):
    """A version of the networkx.Graph class that stores its state in a
    database.

    """
    def __init__(self, gorm, name, data=None, **attr):
        """Call ``_init_atts``, instantiate special mappings, convert ``data``
        argument, and then update graph attributes from kwargs.

        """
        self._init_atts(gorm, name)
        self.adj = GraphSuccessorsMapping(self)
        self.edge = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)


class DiGraph(GormGraph, networkx.DiGraph):
    """A version of the networkx.DiGraph class that stores its state in a
    database.

    """
    def __init__(self, gorm, name, data=None, **attr):
        """Call ``_init_atts``, instantiate special mappings, convert ``data``
        argument, and then update graph attributes from kwargs.

        """
        self._init_atts(gorm, name)
        self.adj = GraphSuccessorsMapping(self)
        self.pred = DiGraphPredecessorsMapping(self)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)

    def remove_edge(self, u, v):
        """Version of remove_edge that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        try:
            del self.succ[u][v]
        except KeyError:
            raise NetworkXError(
                "The edge {}-{} is not in the graph.".format(u, v)
            )

    def remove_edges_from(self, ebunch):
        """Version of remove_edges_from that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        for e in ebunch:
            (u, v) = e[:2]
            if u in self.succ and v in self.succ[u]:
                del self.succ[u][v]

    def add_edge(self, u, v, attr_dict=None, **attr):
        """Version of add_edge that only writes to the database once"""
        if attr_dict is None:
            attr_dict = attr
        else:
            try:
                attr_dict.update(attr)
            except AttributeError:
                raise NetworkXError(
                    "The attr_dict argument must be a dictionary."
                )
        datadict = self.adj[u].get(v, {})
        datadict.update(attr_dict)
        if u not in self.node:
            self.node[u] = {}
        if v not in self.node:
            self.node[v] = {}
        self.succ[u][v] = datadict

    def add_edges_from(self, ebunch, attr_dict=None, **attr):
        """Version of add_edges_from that only writes to the database once"""
        if attr_dict is None:
            attr_dict = attr
        else:
            try:
                attr_dict.update(attr)
            except AttributeError:
                raise NetworkXError(
                    "The attr_dict argument must be a dict."
                )
        for e in ebunch:
            ne = len(e)
            if ne == 3:
                u, v, dd = e
                assert hasattr(dd, "update")
            elif ne == 2:
                u, v = e
                dd = {}
            else:
                raise NetworkXError(
                    "Edge tupse {} must be a 2-tuple or 3-tuple.".format(e)
                )
            if u not in self.node:
                self.node[u] = {}
            if v not in self.node:
                self.node[v] = {}
            datadict = self.adj.get(u, {}).get(v, {})
            datadict.update(attr_dict)
            datadict.update(dd)
            self.succ[u][v] = datadict


class MultiGraph(networkx.MultiGraph, GormGraph):
    """A version of the networkx.MultiGraph class that stores its state in a
    database.

    """
    def __init__(self, gorm, name, data=None, **attr):
        """Call ``_init_atts``, instantiate special mappings, convert ``data``
        argument, and then update graph attributes from kwargs.

        """
        self._init_atts(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        self.edge = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)


class MultiDiGraph(networkx.MultiDiGraph, GormGraph):
    """A version of the networkx.MultiDiGraph class that stores its state in a
    database.

    """
    def __init__(self, gorm, name, data=None, **attr):
        """Call ``_init_atts``, instantiate special mappings, convert ``data``
        argument, and then update graph attributes from kwargs.

        """
        self._init_atts(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        self.pred = MultiDiGraphPredecessorsMapping(gorm, name)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)

    def remove_edge(self, u, v, key=None):
        """Version of remove_edge that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        try:
            d = self.adj[u][v]
        except KeyError:
            raise NetworkXError(
                "The edge {}-{} is not in the graph.".format(u, v)
            )
        if key is None:
            d.popitem()
        else:
            try:
                del d[key]
            except KeyError:
                raise NetworkXError(
                    "The edge {}-{} with key {} is not in the graph.".format
                    (u, v, key)
                )
        if len(d) == 0:
            del self.succ[u][v]

    def remove_edges_from(self, ebunch):
        """Version of remove_edges_from that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        for e in ebunch:
            (u, v) = e[:2]
            if u in self.succ and v in self.succ[u]:
                del self.succ[u][v]

    def add_edge(self, u, v, key=None, attr_dict=None, **attr):
        """Version of add_edge that only writes to the database once."""
        if attr_dict is None:
            attr_dict = attr
        else:
            try:
                attr_dict.update(attr)
            except AttributeError:
                raise NetworkXError(
                    "The attr_dict argument must be a dictionary."
                )
        if u not in self.node:
            self.node[u] = {}
        if v not in self.node:
            self.node[v] = {}
        if v in self.succ[u]:
            keydict = self.adj[u][v]
            if key is None:
                key = len(keydict)
                while key in keydict:
                    key += 1
            datadict = keydict.get(key, {})
            datadict.update(attr_dict)
            keydict[key] = datadict
        else:
            if key is None:
                key = 0
            datadict = {}
            datadict.update(attr_dict)
            keydict = {key: datadict}
            self.succ[u][v] = keydict

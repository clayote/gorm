import networkx
from networkx.exception import NetworkXError
from collections import MutableMapping


class GraphMapping(MutableMapping):
    """Mapping for graph attributes"""
    def __init__(self, graph):
        """Initialize private dict and store pointers to the graph and ORM"""
        self.graph = graph
        self.gorm = graph.gorm

    def __getitem__(self, key):
        """If key is 'graph', return myself as a dict, else get the present
        value of the key and return that

        """
        if key == 'graph':
            return dict(self)
        for (branch, rev) in self.gorm._active_branches():
            self.gorm.cursor.execute(
                "SELECT value, valtype FROM graph_val JOIN ("
                "SELECT graph, key, branch, MAX(rev) AS rev "
                "FROM graph_val WHERE "
                "graph=? AND "
                "key=? AND "
                "branch=? AND "
                "rev<=? GROUP BY graph, key, branch) AS hirev "
                "ON graph_val.graph=hirev.graph "
                "AND graph_val.key=hirev.key "
                "AND graph_val.branch=hirev.branch "
                "AND graph_val.rev=hirev.rev;",
                (
                    self.graph.name,
                    key,
                    branch,
                    rev
                )
            )
            results = self.gorm.cursor.fetchall()
            if len(results) == 0:
                continue
            elif len(results) > 1:
                raise ValueError("Silly data in graph_val table")
            else:
                (value, valtype) = results.pop()
                if valtype == 'unset':
                    raise KeyError("key not set right now")
                return self.gorm.cast(value, valtype)
        raise KeyError("key is not set, ever")

    def __setitem__(self, key, value):
        """Set key=value at the present branch and revision"""
        branch = self.gorm.branch
        rev = self.gorm.rev
        (v, valtyp) = self.gorm.stringify(value)
        # delete first, in case there already is such an assignment
        self.gorm.cursor.execute(
            "DELETE FROM graph_val WHERE graph=? AND key=? AND branch=? AND rev=?;",
            (self.graph.name, key, branch, rev)
        )
        self.gorm.cursor.execute(
            "INSERT INTO graph_val ("
            "graph, "
            "key, "
            "branch, "
            "rev, "
            "value, "
            "valtype) VALUES (?, ?, ?, ?, ?, ?, ?);",
            (
                self.graph.name,
                key,
                branch,
                rev,
                v,
                valtyp
            )
        )

    def __delitem__(self, key):
        """Indicate that the key has no value at this time"""
        branch = self.gorm.branch
        rev = self.gorm.rev
        # if there's already a value, delete it
        self.gorm.cursor.execute(
            "DELETE FROM graph_val WHERE graph=? AND key=? AND branch=? AND rev=?;",
            (self.graph.name, key, branch, rev)
        )
        # type 'unset' means no value here
        self.gorm.cursor.execute(
            "INSERT INTO graph_val (graph, key, branch, rev, valtype) VALUES "
            "(?, ?, ?, ?, 'unset');",
            (self.graph.name, key, branch, rev)
        )

    def __iter__(self):
        """Iterate over the keys that aren't presently of valtype 'unset'"""
        seen = set()
        for (branch, rev) in self.gorm._active_branches():
            self.gorm.cursor.execute(
                "SELECT graph_val.key, graph_val.valtype='unset' "
                "FROM graph_val JOIN ("
                "SELECT graph, key, branch, MAX(rev) AS rev FROM graph_val "
                "WHERE graph=? "
                "AND branch=? "
                "AND rev<=? "
                "GROUP BY graph, key, branch) AS hirev "
                "ON graph_val.graph=hirev.graph "
                "AND graph_val.key=hirev.key "
                "AND graph_val.branch=hirev.branch "
                "AND graph_val.rev=hirev.rev;",
                (
                    self.graph.name,
                    branch,
                    rev
                )
            )
            for row in self.gorm.cursor.fetchall():
                try:
                    key = int(row[0])
                except ValueError:
                    key = row[0]
                if key in seen:
                    continue
                seen.add(key)
                is_unset = bool(row[1])
                if not is_unset:
                    yield key

    def __len__(self):
        """Number of non-'unset' keys"""
        n = 0
        for k in iter(self):
            n += 1
        return n

    def clear(self):
        """Delete everything"""
        for k in self:
            del self[k]

    def __repr__(self):
        """Looks like a dictionary."""
        return repr(dict(self))


class GraphNodeMapping(GraphMapping):
    """Mapping for nodes in a graph"""
    def __init__(self, graph):
        self.graph = graph
        self.gorm = graph.gorm

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

    def __iter__(self):
        """Iterate over the names of the nodes"""
        return self.gorm._iternodes(self.graph.name)

    def __len__(self):
        """How many nodes exist right now?"""
        return self.gorm._countnodes(self.graph.name)

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
            rev=self.gorm.rev
            self.gorm.cursor.execute(
                "DELETE FROM nodes WHERE "
                "graph=? AND "
                "node=? AND "
                "branch=? AND "
                "rev=?;",
                (
                    self.graph.name,
                    self.node,
                    branch,
                    rev
                )
            )
            self.gorm.cursor.execute(
                "INSERT INTO nodes ("
                "graph, "
                "node, "
                "branch, "
                "rev, "
                "extant) VALUES (?, ?, ?, ?, ?);",
                (
                    self.graph.name,
                    self.node,
                    branch,
                    rev,
                    v
                )
            )

        def __init__(self, graph, node):
            """Store name and graph"""
            self.graph = graph
            self.gorm = graph.gorm
            self.node = node
            self.name = self.node

        def __getitem__(self, key):
            """Get the value of the key at the present branch and rev"""
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    "SELECT node_val.value, node_val.valtype FROM node_val JOIN ("
                    "SELECT graph, node, key, branch, MAX(rev) AS rev "
                    "FROM node_val WHERE "
                    "graph=? AND "
                    "node=? AND "
                    "key=? AND "
                    "branch=? AND "
                    "rev<=? "
                    "GROUP BY graph, node, key, branch) AS hirev "
                    "ON node_val.graph=hirev.graph "
                    "AND node_val.node=hirev.node "
                    "AND node_val.key=hirev.key "
                    "AND node_val.branch=hirev.branch "
                    "AND node_val.rev=hirev.rev;",
                    (
                        self.graph.name,
                        self.node,
                        key,
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
                    (value, valtype) = data.pop()
                    if valtype == 'unset':
                        raise KeyError("key unset at the moment")
                    return self.gorm.cast(value, valtype)
            raise KeyError("key never set")

        def __iter__(self):
            """Iterate over those keys that do not have valtype='unset' at the
            moment

            """
            seen = set()
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    "SELECT node_val.key, node_val.valtype='unset' FROM node_val JOIN ("
                    "SELECT graph, node, key, branch, MAX(rev) AS rev "
                    "FROM node_val WHERE "
                    "graph=? AND "
                    "node=? AND "
                    "branch=? AND "
                    "rev<=? "
                    "GROUP BY graph, node, key, branch) AS hirev ON "
                    "node_val.graph=hirev.graph AND "
                    "node_val.node=hirev.node AND "
                    "node_val.key=hirev.key AND "
                    "node_val.branch=hirev.branch AND "
                    "node_val.rev=hirev.rev;",
                    (
                        self.graph.name,
                        self.node,
                        branch,
                        rev
                    )
                )
                for (key, valtype) in self.gorm.cursor.fetchall():
                    try:
                        key = int(key)
                    except ValueError:
                        pass
                    if key not in seen and valtype != 'unset':
                        yield key
                    seen.add(key)

        def __setitem__(self, key, value):
            """Set key=value at the present branch and rev. Overwrite if
            necessary.

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            (v, valtyp) = self.gorm.stringify(value)
            self.gorm.cursor.execute(
                "DELETE FROM node_val WHERE graph=? AND node=? AND key=? AND branch=? AND rev=?;",
                (self.graph.name, self.node, key, branch, rev)
            )
            self.gorm.cursor.execute(
                "INSERT INTO node_val ("
                "graph, "
                "node, "
                "key, "
                "branch, "
                "rev, "
                "value, "
                "valtype) VALUES "
                "(?, ?, ?, ?, ?, ?);",
                (
                    self.graph.name,
                    self.node,
                    key,
                    branch,
                    rev,
                    v,
                    valtyp
                )
            )

        def __delitem__(self, key):
            """Set the key's valtype to 'unset', indicating it should be ignored
            now and in future revs

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "DELETE FROM node_val WHERE graph=? AND node=? AND key=? AND branch=? AND rev=?;",
                (self.graph.name, self.node, key, branch, rev)
            )
            self.gorm.cursor.execute(
                "INSERT INTO node_val (graph, node, key, branch, rev, valtype) VALUES "
                "(?, ?, ?, ?, 'unset');",
                (self.graph.name, self.node, key, branch, rev)
            )

        def clear(self):
            """Delete everything and stop existing"""
            for k in self:
                del self[k]
            self.exists = False


class GraphEdgeMapping(GraphMapping):
    """Provides an adjacency mapping and possibly a predecessor mapping
    for a graph.

    """
    def __init__(self, graph):
        """Store the graph"""
        self.graph = graph
        self.gorm = graph.gorm

    def __iter__(self):
        """Iterate over the nodes that exist at the moment"""
        return self.gorm._iternodes(self.graph.name)

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

    class Edge(GraphMapping):
        """Mapping for edge attributes"""
        def __init__(self, graph, nodeA, nodeB, idx=0):
            """Store the graph, the names of the nodes, and the index.

            For non-multigraphs the index is always 0.

            """
            self.graph = graph
            self.gorm = graph.gorm
            try:
                self.nodeA = int(nodeA)
            except ValueError:
                self.nodeA = nodeA
            try:
                self.nodeB = int(nodeB)
            except ValueError:
                self.nodeB = nodeB
            self.idx = idx

        @property
        def exists(self):
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    "SELECT edges.extant FROM edges JOIN ("
                    "SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev FROM edges "
                    "WHERE graph=? "
                    "AND nodeA=? "
                    "AND nodeB=? "
                    "AND idx=? "
                    "AND branch=? "
                    "AND rev<=? "
                    "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev "
                    "ON edges.graph=hirev.graph "
                    "AND edges.nodeA=hirev.nodeA "
                    "AND edges.nodeB=hirev.nodeB "
                    "AND edges.idx=hirev.idx "
                    "AND edges.branch=hirev.branch "
                    "AND edges.rev=hirev.rev;",
                    (
                        self.graph.name,
                        self.nodeA,
                        self.nodeB,
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
            self.gorm.cursor.execute(
                "DELETE FROM edges WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "nodeB=? AND "
                "idx=? AND "
                "branch=? AND "
                "rev=?;",
                (
                    self.graph.name,
                    str(self.nodeA),
                    str(self.nodeB),
                    self.idx,
                    branch,
                    rev
                )
            )
            self.gorm.cursor.execute(
                "INSERT INTO edges ("
                "graph, "
                "nodeA, "
                "nodeB, "
                "idx, "
                "branch, "
                "rev, "
                "extant) VALUES (?, ?, ?, ?, ?, ?, ?);",
                (
                    self.graph.name,
                    str(self.nodeA),
                    str(self.nodeB),
                    self.idx,
                    branch,
                    rev,
                    v
                )
            )

        def __getitem__(self, key):
            """Return the present value of the key, or raise KeyError if it's
            unset

            """
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    "SELECT edge_val.value, edge_val.valtype FROM edge_val JOIN ("
                    "SELECT graph, nodeA, nodeB, idx, key, branch, MAX(rev) AS rev "
                    "FROM edge_val WHERE "
                    "graph=? AND "
                    "nodeA=? AND "
                    "nodeB=? AND "
                    "idx=? AND "
                    "key=? AND "
                    "branch=? AND "
                    "rev<=? "
                    "GROUP BY graph, nodeA, nodeB, idx, key, branch) AS hirev "
                    "ON edge_val.graph=hirev.graph "
                    "AND edge_val.nodeA=hirev.nodeA "
                    "AND edge_val.nodeB=hirev.nodeB "
                    "AND edge_val.idx=hirev.idx "
                    "AND edge_val.key=hirev.key "
                    "AND edge_val.branch=hirev.branch "
                    "AND edge_val.rev=hirev.rev;",
                    (
                        self.graph.name,
                        self.nodeA,
                        self.nodeB,
                        self.idx,
                        key,
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
                    (value, valtype) = data.pop()
                    if valtype == 'unset':
                        raise KeyError("key not set at the moment")
                    return self.gorm.cast(value, valtype)
            raise KeyError('key never set')

        def __iter__(self):
            """Yield those keys that have a real value, not of valtype 'unset'"""
            seen = set()
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    "SELECT edge_val.key, edge_val.valtype='unset' FROM edge_val JOIN ("
                    "SELECT graph, nodeA, nodeB, idx, key, branch, MAX(rev) AS rev "
                    "FROM edge_val WHERE "
                    "graph=? AND "
                    "nodeA=? AND "
                    "nodeB=? AND "
                    "idx=? AND "
                    "branch=? AND "
                    "rev<=? GROUP BY graph, nodeA, nodeB, idx, key, branch) AS hirev "
                    "ON edge_val.graph=hirev.graph "
                    "AND edge_val.nodeA=hirev.nodeA "
                    "AND edge_val.nodeB=hirev.nodeB "
                    "AND edge_val.idx=hirev.idx "
                    "AND edge_val.rev=hirev.rev;",
                    (
                        self.graph.name,
                        self.nodeA,
                        self.nodeB,
                        self.idx,
                        branch,
                        rev
                    )
                )
                for (key, valtype) in self.gorm.cursor.fetchall():
                    if key not in seen and valtype != 'unset':
                        yield key
                    seen.add(key)

        def __setitem__(self, key, value):
            """Set a database record to say that key=value at the present branch
            and revision

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            (v, valtyp) = self.gorm.stringify(value)
            self.gorm.cursor.execute(
                "DELETE FROM edge_val WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "nodeB=? AND "
                "idx=? AND "
                "key=? AND "
                "branch=? AND "
                "rev=?;",
                (
                    self.graph.name,
                    str(self.nodeA),
                    str(self.nodeB),
                    self.idx,
                    key,
                    branch,
                    rev
                )
            )
            self.gorm.cursor.execute(
                "INSERT INTO edge_val ("
                "graph, "
                "nodeA, "
                "nodeB, "
                "idx, "
                "key, "
                "branch, "
                "rev, "
                "value, "
                "valtype) VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    self.graph.name,
                    str(self.nodeA),
                    str(self.nodeB),
                    self.idx,
                    key,
                    branch,
                    rev,
                    v,
                    valtyp
                )
            )

        def __delitem__(self, key):
            """Set the key's valtype to 'unset', such that it is not yielded by
            ``__iter__``

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "DELETE FROM edge_val WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "nodeB=? AND "
                "idx=? AND "
                "key=? AND "
                "branch=? AND "
                "rev=?;",
                (
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.idx,
                    key,
                    branch,
                    rev
                )
            )
            self.gorm.cursor.execute(
                "INSERT INTO edge_val ("
                "graph, "
                "nodeA, "
                "nodeB, "
                "idx, "
                "key, "
                "branch, "
                "rev, "
                "valtype) VALUES ( ?, ?, ?, ?, ?, ?, ?, 'unset');",
                (
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.idx,
                    key,
                    branch,
                    rev,
                )
            )

        def clear(self):
            for k in self:
                del self[k]
            self.exists = False


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

    class Successors(GraphEdgeMapping):
        def _getsub(self, nodeB):
            """Return what I map to"""
            return self.Edge(self.graph, self.nodeA, nodeB)

        def __init__(self, container, nodeA):
            """Store container and node"""
            self.container = container
            self.graph = container.graph
            self.gorm = self.graph.gorm
            self.nodeA = nodeA

        def __iter__(self):
            """Iterate over node IDs that have an edge with my nodeA"""
            seen = set()
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    "SELECT edges.nodeB, edges.extant FROM edges JOIN ("
                    "SELECT graph, nodeA, nodeB, branch, MAX(rev) AS rev "
                    "FROM edges WHERE "
                    "graph=? AND "
                    "nodeA=? AND "
                    "branch=? AND "
                    "rev<=? "
                    "GROUP BY graph, nodeA, nodeB, branch) "
                    "AS hirev ON "
                    "edges.graph=hirev.graph AND "
                    "edges.nodeA=hirev.nodeA AND "
                    "edges.nodeB=hirev.nodeB AND "
                    "edges.branch=hirev.branch AND "
                    "edges.rev=hirev.rev;",
                    (
                        self.graph.name,
                        self.nodeA,
                        branch,
                        rev
                    )
                )
                for row in self.gorm.cursor.fetchall():
                    try:
                        nodeB = int(row[0])
                    except ValueError:
                        nodeB = row[0]
                    extant = bool(row[1])
                    if nodeB not in seen and extant:
                        yield nodeB
                    seen.add(nodeB)

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
            self.nodeB = nodeB

        def __iter__(self):
            """Iterate over the edges that exist at the present (branch, rev)"""
            seen = set()
            for (branch, rev) in self.gorm._active_branches():
                self.gorm.cursor.execute(
                    "SELECT edges.nodeA, edges.extant FROM edges JOIN ("
                    "SELECT graph, nodeA, nodeB, branch, MAX(rev) AS rev "
                    "FROM edges WHERE "
                    "graph=? AND "
                    "nodeB=? AND "
                    "branch=? AND "
                    "rev=? "
                    "GROUP BY graph, nodeA, nodeB, branch "
                    ") AS hirev ON "
                    "edges.graph=hirev.graph AND "
                    "edges.nodeA=hirev.nodeA AND "
                    "edges.nodeB=hirev.nodeB AND "
                    "edges.branch=hirev.branch AND "
                    "edges.rev=hirev.rev;",
                    (
                        self.graph.name,
                        self.nodeB,
                        branch,
                        rev
                    )
                )
                for row in self.gorm.cursor.fetchall():
                    try:
                        nodeA = int(row[0])
                    except ValueError:
                        nodeA = row[0]
                    extant = bool(row[1])
                    if nodeA not in seen and extant:
                        yield nodeA
                    seen.add(nodeA)

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
        self.nodeA = nodeA
        self.nodeB = nodeB

    def __iter__(self):
        """Iterate over the indices of existing edges in ascending order"""
        branches = self.gorm.active_branches
        rev = self.gorm.rev
        self.gorm.cursor.execute(
            "SELECT edges.branch, edges.idx, edges.extant FROM edges JOIN ("
            "SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
            "FROM edges WHERE "
            "graph=? AND "
            "nodeA=? AND "
            "nodeB=? AND "
            "rev<=? AND "
            "branch IN ({qms}) "
            "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev ON "
            "edges.graph=hirev.graph AND "
            "edges.nodeA=hirev.nodeA AND "
            "edges.nodeB=hirev.nodeB AND "
            "edges.idx=hirev.idx AND "
            "edges.branch=hirev.branch AND "
            "edges.rev=hirev.rev;".format(
                qms=", ".join("?" * len(branches))
            ), (
                self.graph.name,
                self.nodeA,
                self.nodeB,
                rev
            ) + branches
        )
        d = {}
        for row in self.gorm.cursor.fetchall():
            branch = branches.index(row[0])
            idx = row[1]
            extant = bool(row[2])
            if idx not in d or d[idx][0] < branch:
                d[idx] = (branch, extant)
        for idx in sorted(d.values()):
            for (branch, extant) in d[idx]:
                if extant:
                    yield idx

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


class Graph(networkx.Graph):
    def __init__(self, gorm, name, data=None, **attr):
        """A version of the networkx.Graph class that stores its state in a
        database.

        For the most part, works just like networkx.Graph, but you
        can't change its name after creation, and you can't assign
        None as the value of any key--or rather, doing so is
        considered eqivalent to deleting the key altogether.

        """
        self._name = name
        self.gorm = gorm
        self.graph = GraphMapping(self)
        self.keys = self.graph.keys
        self.values = self.graph.values
        self.items = self.graph.items
        self.node = GraphNodeMapping(self)
        self.adj = GraphSuccessorsMapping(self)
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, v):
        raise TypeError("gorm graphs can't be renamed")

    def clear(self):
        """Remove all nodes and edges from the graph.

        Unlike the regular networkx implementation, this does *not*
        remove the graph's name. But all the other graph, node, and
        edge attributes go away.

        """
        self.adj.clear()
        self.node.clear()
        self.graph.clear()


class DiGraph(networkx.DiGraph):
    def __init__(self, gorm, name, data=None, **attr):

        """A version of the networkx.DiGraph class that stores its state in a
        database.

        For the most part, works just like networkx.DiGraph, but you
        can't change its name after creation, and you can't assign
        None as the value of any key--or rather, doing so is
        considered eqivalent to deleting the key altogether.

        """
        self.gorm = gorm
        self._name = name
        self.graph = GraphMapping(self)
        self.keys = self.graph.keys
        self.iterkeys = self.graph.iterkeys
        self.values = self.graph.values
        self.itervalues = self.graph.itervalues
        self.items = self.graph.items
        self.iteritems = self.graph.iteritems
        self.node = GraphNodeMapping(self)
        self.adj = GraphSuccessorsMapping(self)
        self.pred = DiGraphPredecessorsMapping(self)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, v):
        raise TypeError("gorm graphs can't be renamed")

    def remove_edge(self, u, v):
        """Version of remove_edge that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        try:
            del self.succ[u][v]
        except KeyError:
            raise NetworkXError("The edge {}-{} is not in the graph.".format(u, v))

    def remove_edges_from(self, ebunch):
        """Version of remove_edges_from that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        for e in ebunch:
            (u, v) = e[:2]
            if u in self.succ and v in self.succ[u]:
                del self.succ[u][v]


class MultiGraph(networkx.MultiGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self._name = name
        self.graph = GraphMapping(gorm, name)
        self.keys = self.graph.keys
        self.iterkeys = self.graph.iterkeys
        self.values = self.graph.values
        self.itervalues = self.graph.itervalues
        self.items = self.graph.items
        self.iteritems = self.graph.iteritems
        self.node = GraphNodeMapping(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, v):
        raise TypeError("gorm graphs can't be renamed")


class MultiDiGraph(networkx.MultiDiGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self._name = name
        self.graph = GraphMapping(gorm, name)
        self.keys = self.graph.keys
        self.iterkeys = self.graph.iterkeys
        self.values = self.graph.values
        self.itervalues = self.graph.itervalues
        self.items = self.graph.items
        self.iteritems = self.graph.iteritems
        self.node = GraphNodeMapping(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        self.pred = MultiDiGraphPredecessorsMapping(gorm, name)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj
 
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, v):
        raise TypeError("gorm graphs can't be renamed")

    def remove_edge(self, u, v, key=None):
        """Version of remove_edge that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        try:
            d = self.adj[u][v]
        except KeyError:
            raise NetworkXError("The edge {}-{} is not in the graph.".format(u, v))
        if key is None:
            d.popitem()
        else:
            try:
                del d[key]
            except KeyError:
                raise NetworkXError(
                    "The edge {}-{} with key {} is not in the graph.".format(u, v, key)
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

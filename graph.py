import networkx
from networkx.exception import NetworkXError
from collections import MutableMapping, defaultdict


class GraphMapping(MutableMapping):
    """Mapping for graph attributes"""
    def __init__(self, graph):
        """Initialize private dict and store pointers to the graph and ORM"""
        self.graph = graph
        self.gorm = graph.gorm

    def __getitem__(self, key):
        if key == 'graph':
            return dict(self)
        rev = self.gorm.rev
        branches = tuple(self.gorm._active_branches())
        self.gorm.cursor.execute(
            "SELECT value, valtype FROM graph_val JOIN ("
            "SELECT graph, key, MAX(rev) AS rev "
            "FROM graph_val WHERE "
            "graph=? AND "
            "key=? AND "
            "rev<=? AND "
            "branch IN ({qms}) AND "
            "valtype<>'unset' "
            "GROUP BY graph, key) AS hirev "
            "ON graph_val.graph=hirev.graph "
            "AND graph_val.key=hirev.key "
            "WHERE branch IN ({qms});".format(
                qms=", ".join("?" * len(branches))
            ),
            (
                self.graph.name,
                key,
                rev
            ) + branches * 2
        )
        try:
            return self.gorm.cast(*self.gorm.cursor.fetchone())
        except TypeError:
            raise KeyError("Key not set for graph")

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
        rev = self.gorm.rev
        branches = tuple(self.gorm._active_branches())
        self.gorm.cursor.execute(
            "SELECT graph_val.key FROM graph_val JOIN ("
            "SELECT graph, key, MAX(rev) AS rev "
            "FROM graph_val WHERE "
            "graph=? AND "
            "rev<=? AND "
            "branch IN ({qms}) AND "
            "valtype<>'unset' "
            "GROUP BY graph, key) AS hirev ON "
            "graph_val.graph=hirev.graph AND "
            "graph_val.rev=hirev.rev WHERE "
            "branch IN ({qms});".format(
                qms=", ".join("?" * len(branches))
            ), (
                self.graph.name,
                rev
            ) + branches * 2
        )
        for row in self.gorm.cursor.fetchall():
            try:
                yield int(row[0])
            except ValueError:
                yield row[0]

    def __len__(self):
        rev = self.gorm.rev
        branches = tuple(self.gorm._active_branches())
        self.gorm.cursor.execute(
            "SELECT COUNT(graph_val.key) FROM graph_val JOIN ("
            "SELECT graph, key, MAX(rev) AS rev "
            "FROM graph_val WHERE "
            "graph=? AND "
            "rev<=? AND "
            "branch IN ({qms}) AND "
            "valtype<>'unset' "
            "GROUP BY graph, key) AS hirev ON "
            "graph_val.graph=hirev.graph AND "
            "graph_val.rev=hirev.rev WHERE "
            "branch IN ({qms});".format(
                qms=", ".join("?" * len(branches))
            ), (
                self.graph.name,
                rev
            ) + branches * 2
        )
        return int(self.gorm.cursor.fetchone()[0])

    def clear(self):
        """Delete everything"""
        for k in self:
            del self[k]

    def __repr__(self):
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
        return self.gorm._iternodes(self.graph.name)

    def __len__(self):
        return self.gorm._countnodes(self.graph.name)

    def __eq__(self, other):
        if not hasattr(other, 'keys'):
            return False
        if set(self.keys()) != set(other.keys()):
            return False
        for k in self.iterkeys():
            if dict(self[k]) != dict(other[k]):
                return False
        return True


    class Node(GraphMapping):
        """Mapping for node attributes"""

        @property
        def exists(self):
            branches = tuple(self.gorm._active_branches())
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "SELECT nodes.extant FROM nodes JOIN ("
                "SELECT graph, node, MAX(rev) AS rev "
                "FROM nodes WHERE "
                "graph=? AND "
                "node=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, node) AS hirev "
                "ON nodes.graph=hirev.graph "
                "AND nodes.node=hirev.node "
                "AND nodes.rev=hirev.rev "
                "WHERE branch IN ({qms});".format(
                    qms=", ".join("?" * len(branches))
                ), (
                    self.graph.name,
                    self.node,
                    rev
                ) + branches * 2
            )
            try:
                return bool(self.gorm.cursor.fetchone()[0])
            except TypeError:
                return False

        @exists.setter
        def exists(self, v):
            if not isinstance(v, bool):
                raise TypeError("Existence is boolean")
            branch = self.gorm.branch
            rev=self.gorm.rev
            (name, nametype) = self.gorm.stringify(self.node)
            self.gorm.cursor.execute(
                "DELETE FROM nodes WHERE "
                "graph=? AND "
                "node=? AND "
                "branch=? AND "
                "rev=?;",
                (
                    self.graph.name,
                    name,
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
                "extant, "
                "nametype) VALUES (?, ?, ?, ?, ?, ?);",
                (
                    self.graph.name,
                    name,
                    branch,
                    rev,
                    v,
                    nametype
                )
            )

        def __init__(self, graph, node):
            self.graph = graph
            self.gorm = graph.gorm
            self.node = node
            self.name = self.node

        def __getitem__(self, key):
            rev = self.gorm.rev
            branches = tuple(self.gorm._active_branches())
            self.gorm.cursor.execute(
                "SELECT node_val.value, node_val.type FROM node_val JOIN ("
                "SELECT graph, node, key, MAX(rev) AS rev "
                "FROM node_val WHERE "
                "graph=? AND "
                "node=? AND "
                "key=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "AND type<>'unset' "
                "GROUP BY graph, node, key) AS hirev "
                "ON node_val.graph=hirev.graph "
                "AND node_val.node=hirev.node "
                "AND node_val.key=hirev.key "
                "AND node_val.rev=hirev.rev "
                "WHERE branch IN ({qms});".format(
                    qms=", ".join("?" * len(branches))
                ),
                (self.graph.name, self.node, key, rev) + branches * 2
            )
            return self.gorm.cast(*self.gorm.cursor.fetchone())

        def __iter__(self):
            rev = self.gorm.rev
            branches = tuple(self.gorm._active_branches())
            self.gorm.cursor.execute(
                "SELECT node_val.key FROM node_val JOIN "
                "(SELECT graph, node, key, MAX(rev) AS rev "
                "FROM node_val WHERE "
                "graph=? AND "
                "node=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, node, key) AS hirev "
                "ON node_val.graph=hirev.graph "
                "AND node_val.node=hirev.node "
                "AND node_val.key=hirev.key "
                "AND node_val.rev=hirev.rev "
                "WHERE node_val.valtype<>'unset' "
                "AND branch IN ({qms});".format(
                    qms=", ".join("?" * len(branches))
                ), (
                    self.graph.name,
                    self.node,
                    rev
                ) + branches * 2
            )
            for row in self.gorm.cursor.fetchall():
                try:
                    yield int(row[0])
                except ValueError:
                    yield row[0]

        def __setitem__(self, key, value):
            """Set key=value at the present branch and revision. Overwrite if necessary."""
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
        self.graph = graph
        self.gorm = graph.gorm

    def __iter__(self):
        return self.gorm._iternodes(self.graph.name)

    def __len__(self):
        return self.gorm._countnodes(self.graph.name)

    def __eq__(self, other):
        if not hasattr(other, 'keys'):
            return False
        myks = set(self.keys())
        if myks != set(other.keys()):
            return False
        # not really sure why, but if I iterate over myself rather
        # than myks I don't really iterate over all the keys
        for k in myks:
            if dict(self[k]) != dict(other[k]):
                return False
        return True

    class Edge(GraphMapping):
        def __init__(self, graph, nodeA, nodeB, idx=0):
            self.graph = graph
            self.gorm = graph.gorm
            self.nodeA = nodeA
            self.nodeB = nodeB
            self.idx = idx

        @property
        def exists(self):
            branches = tuple(self.gorm._active_branches())
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "SELECT edges.extant FROM edges JOIN ( "
                "SELECT graph, nodeA, nodeB, idx, MAX(rev) AS rev "
                "FROM edges WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "nodeB=? AND "
                "idx=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, nodeA, nodeB, idx) AS hirev "
                "ON edges.graph=hirev.graph "
                "AND edges.nodeA=hirev.nodeA "
                "AND edges.nodeB=hirev.nodeB "
                "AND edges.idx=hirev.idx "
                "AND edges.rev=hirev.rev "
                "WHERE branch IN ({qms});".format(
                    qms=", ".join("?" * len(branches))
                ), (
                    self.graph.name,
                    unicode(self.nodeA),
                    unicode(self.nodeB),
                    self.idx,
                    rev
                ) + branches * 2
            )
            try:
                return bool(self.gorm.cursor.fetchone()[0])
            except TypeError:  # no record
                return False

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
                    unicode(self.nodeA),
                    unicode(self.nodeB),
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
                    unicode(self.nodeA),
                    unicode(self.nodeB),
                    self.idx,
                    branch,
                    rev,
                    v
                )
            )

        def __getitem__(self, key):
            rev = self.gorm.rev
            branches = tuple(self.gorm._active_branches())
            self.gorm.cursor.execute(
                "SELECT edge_val.value, edge_val.valtype FROM edge_val JOIN "
                "(SELECT graph, nodeA, nodeB, idx, key, MAX(rev) AS rev "
                "FROM edge_val WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "nodeB=? AND "
                "idx=? AND "
                "key=? AND "
                "rev<=? AND "
                "branch IN ({qms})) AS hirev "
                "ON edge_val.graph=hirev.graph "
                "AND edge_val.nodeA=hirev.nodeA "
                "AND edge_val.nodeB=hirev.nodeB "
                "AND edge_val.idx=hirev.idx "
                "AND edge_val.key=hirev.key "
                "AND edge_val.rev=hirev.rev "
                "WHERE edge_val.valtype<>'unset';".format(
                    qms=", ".join("?" * len(branches))
                ),
                (
                    self.graph.name,
                    unicode(self.nodeA),
                    unicode(self.nodeB),
                    self.idx,
                    unicode(key),
                    rev
                ) + branches
            )
            return self.gorm.cast(*self.gorm.cursor.fetchone())

        def __iter__(self):
            rev = self.gorm.rev
            branches = tuple(self.gorm._active_branches())
            self.gorm.cursor.execute(
                "SELECT DISTINCT edge_val.key FROM edge_val JOIN ("
                "SELECT graph, nodeA, nodeB, idx, key, MAX(rev) AS rev "
                "FROM edge_val WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "nodeB=? AND "
                "idx=? AND "
                "rev<=? AND "
                "branch IN ({qms})"
                "GROUP BY graph, nodeA, nodeB, idx, key) AS hirev "
                "ON edge_val.graph=hirev.graph "
                "AND edge_val.nodeA=hirev.nodeA "
                "AND edge_val.nodeB=hirev.nodeB "
                "AND edge_val.idx=hirev.idx "
                "AND edge_val.rev=hirev.rev "
                "AND edge_val.key=hirev.key "
                "WHERE edge_val.valtype<>'unset' "
                "AND edge_val.branch IN ({qms});".format(
                    qms=", ".join("?" * len(branches))
                ), (
                    self.graph.name,
                    unicode(self.nodeA),
                    unicode(self.nodeB),
                    self.idx,
                    rev
                ) + branches * 2
            )
            for row in self.gorm.cursor.fetchall():
                try:
                    yield int(row[0])
                except ValueError:
                    yield row[0]

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
                    unicode(self.nodeA),
                    unicode(self.nodeB),
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
                    unicode(self.nodeA),
                    unicode(self.nodeB),
                    self.idx,
                    key,
                    branch,
                    rev,
                    v,
                    valtyp
                )
            )

        def __delitem__(self, key):
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
    def __getitem__(self, nodeA):
        if not self.gorm._node_exists(self.graph.name, nodeA):
            raise KeyError("No such node")
        return self.Successors(self, nodeA)

    def __setitem__(self, nodeA, val):
        sucs = self.Successors(self, nodeA)
        sucs.clear()
        sucs.update(val)

    def __delitem__(self, nodeA):
        self.Successors(self, nodeA).clear()

    class Successors(GraphEdgeMapping):
        def __init__(self, container, nodeA):
            self.container = container
            self.graph = container.graph
            self.gorm = self.graph.gorm
            self.nodeA = nodeA

        def __iter__(self):
            branches = tuple(self.gorm._active_branches())
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "SELECT DISTINCT edges.nodeB FROM edges JOIN "
                "(SELECT graph, nodeA, nodeB, idx, MAX(rev) AS rev "
                "FROM edges WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, nodeA, nodeB, idx) AS hirev "
                "ON edges.graph=hirev.graph "
                "AND edges.nodeA=hirev.nodeA "
                "AND edges.nodeB=hirev.nodeB "
                "AND edges.idx=hirev.idx "
                "AND edges.rev=hirev.rev "
                "WHERE edges.extant={true} "
                "AND branch IN ({qms});".format(
                    qms=", ".join("?" * len(branches)),
                    true=self.gorm.sql_types[self.gorm.sql_flavor]['true']
                ), (
                    self.graph.name,
                    self.nodeA,
                    rev
                ) + branches * 2
            )
            for row in self.gorm.cursor.fetchall():
                try:
                    yield int(row[0])
                except ValueError:
                    yield row[0]

        def __len__(self):
            branches = tuple(self.gorm._active_branches())
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "SELECT COUNT(DISTINCT edges.nodeB) FROM edges JOIN "
                "(SELECT graph, nodeA, nodeB, idx, MAX(rev) AS rev "
                "FROM edges WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, nodeA, nodeB, idx) AS hirev "
                "ON edges.graph=hirev.graph "
                "AND edges.nodeA=hirev.nodeA "
                "AND edges.nodeB=hirev.nodeB "
                "AND edges.idx=hirev.idx "
                "AND edges.rev=hirev.rev "
                "WHERE edges.extant={true} "
                "AND branch IN ({qms});".format(
                    qms=", ".join("?" * len(branches)),
                    true=self.gorm.sql_types[self.gorm.sql_flavor]['true']
                ), (
                    self.graph.name,
                    self.nodeA,
                    rev
                ) + branches * 2
            )
            return int(self.gorm.cursor.fetchone()[0])

        def __getitem__(self, nodeB):
            r = self.Edge(self.graph, self.nodeA, nodeB)
            if not r.exists:
                raise KeyError("Edge doesn't exist")
            return r

        def __setitem__(self, nodeB, value):
            e = self.Edge(self.graph, self.nodeA, nodeB)
            e.clear()
            e.exists = True
            e.update(value)

        def __delitem__(self, nodeB):
            e = self.Edge(self.graph, self.nodeA, nodeB)
            if not e.exists:
                raise KeyError("No such edge")
            e.clear()

        def clear(self):
            for nodeB in self:
                del self[nodeB]


class DiGraphPredecessorsMapping(GraphEdgeMapping):
    def __getitem__(self, nodeB):
        if not self.gorm._node_exists(self.graph.name, nodeB):
            raise KeyError("No such node")
        return self.Predecessors(self, nodeB)

    def __setitem__(self, nodeB, val):
        preds = self.Predecessors(self, nodeB)
        preds.clear()
        preds.update(val)

    def __delitem__(self, nodeB):
        self.Predecessors(self, nodeB).clear()

    class Predecessors(GraphEdgeMapping):
        def _getsub(self, nodeA):
            return self.Edge(self.graph, nodeA, self.nodeB)

        def __init__(self, container, nodeB):
            self.container = container
            self.graph = container.graph
            self.gorm = self.graph.gorm
            self.nodeB = nodeB

        def __iter__(self):
            branches = tuple(self.gorm._active_branches())
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "SELECT DISTINCT edges.nodeA FROM edges JOIN "
                "(SELECT graph, nodeA, nodeB, idx, MAX(rev) AS rev "
                "FROM edges WHERE "
                "graph=? AND "
                "nodeB=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, nodeA, nodeB, idx) AS hirev "
                "ON edges.graph=hirev.graph "
                "AND edges.nodeA=hirev.nodeA "
                "AND edges.nodeB=hirev.nodeB "
                "AND edges.idx=hirev.idx "
                "AND edges.rev=hirev.rev "
                "WHERE edges.extant={true} "
                "AND edges.branch IN ({qms});".format(
                    qms=", ".join("?" * len(branches)),
                    true=self.gorm.sql_types[self.gorm.sql_flavor]['true']
                ), (
                    self.graph.name,
                    self.nodeB,
                    rev
                ) + branches * 2
            )
            for row in self.gorm.cursor.fetchall():
                try:
                    yield int(row[0])
                except ValueError:
                    yield row[0]

        def __len__(self):
            branches = tuple(self.gorm._active_branches())
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "SELECT COUNT(DISTINCT edges.nodeA) FROM edges JOIN "
                "(SELECT graph, nodeA, nodeB, idx, MAX(rev) AS rev "
                "FROM edges WHERE "
                "graph=? AND "
                "nodeB=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, nodeA, nodeB, idx) AS hirev "
                "ON edges.graph=hirev.graph "
                "AND edges.nodeA=hirev.nodeA "
                "AND edges.nodeB=hirev.nodeB "
                "AND edges.idx=hirev.idx "
                "AND edges.rev=hirev.rev "
                "WHERE edges.extant={true} "
                "AND edges.branch IN ({qms});".format(
                    qms=", ".join("?" * len(branches)),
                    true=self.gorm.sql_types[self.gorm.sql_flavor]['true']
                ), (
                    self.graph.name,
                    self.nodeB,
                    rev
                ) + branches * 2
            )
            return int(self.gorm.cursor.fetchone()[0])

        def __getitem__(self, nodeA):
            r = self._getsub(nodeA)
            if not r.exists:
                raise KeyError("Edge doesn't exist")
            return r

        def __setitem__(self, nodeA, value):
            e = self._getsub(nodeA)
            e.clear()
            e.exists = True
            e.update(value)

        def __delitem__(self, nodeA):
            e = self._getsub(nodeA)
            if not e.exists:
                raise KeyError("No such edge")
            e.clear()


class MultiEdges(GraphEdgeMapping):
    def __init__(self, graph, nodeA, nodeB):
        self.graph = graph
        self.gorm = graph.gorm
        self.nodeA = nodeA
        self.nodeB = nodeB

    def __iter__(self):
        branches = tuple(self.gorm._active_branches())
        rev = self.gorm.rev
        self.gorm.cursor.execute(
            "SELECT DISTINCT idx FROM edges JOIN "
            "(SELECT graph, nodeA, nodeB, idx, MAX(rev) AS rev "
            "FROM edges WHERE "
            "graph=? AND "
            "nodeA=? AND "
            "nodeB=? AND "
            "rev<=? AND "
            "branch IN ({qms}) AND"
            "extant={true} "
            "GROUP BY graph, nodeA, nodeB, idx) AS hirev "
            "ON edges.graph=hirev.graph "
            "AND edges.nodeA=hirev.nodeA "
            "AND edges.nodeB=hirev.nodeB "
            "AND edges.idx=hirev.idx "
            "AND edges.rev=hirev.rev "
            "WHERE branch IN ({qms});".format(
                qms=", ".join("?" * len(branches)),
                true=self.gorm.sql_types[self.gorm.sql_flavor]['true']
            ), (
                self.graph.name,
                self.nodeA,
                self.nodeB,
                rev
            ) + branches * 2
        )
        for row in self.gorm.cursor:
            yield int(row[0])

    def __len__(self):
        branches = tuple(self.gorm._active_branches())
        rev = self.gorm.rev
        self.gorm.cursor.execute(
            "SELECT COUNT(DISTINCT idx) FROM edges JOIN "
            "(SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
            "FROM edges WHERE "
            "graph=? AND "
            "nodeA=? AND "
            "nodeB=? AND "
            "rev<=? AND "
            "branch IN ({qms}) "
            "GROUP BY graph, nodeA, nodeB, idx) AS hirev "
            "ON edges.graph=hirev.graph "
            "AND edges.nodeA=hirev.nodeA "
            "AND edges.nodeB=hirev.nodeB "
            "AND edges.rev=hirev.rev "
            "WHERE edges.extant={true} "
            "AND branch IN ({qms});".format(
                qms=", ".join("?" * len(branches)),
                true=self.gorm.sql_types[self.gorm.sql_flavor['true']]
            ), (
                self.graph.name,
                self.nodeA,
                self.nodeB,
                rev
            ) + branches * 2
        )
        return self.gorm.cursor.fetchone()[0]

    def __getitem__(self, idx):
        r = self.Edge(self.graph, self.nodeA, self.nodeB, idx)
        if not r.exists:
            raise KeyError("No edge at that index")
        return r

    def __setitem__(self, idx, val):
        e = self.Edge(self.graph, self.nodeA, self.nodeB, idx)
        e.clear()
        e.exists = True
        e.update(val)

    def __delitem__(self, idx):
        e = self.Edge(self.graph, self.nodeA, self.nodeB, idx)
        if not e.exists:
            raise KeyError("No edge at that index")
        e.clear()

    def clear(self):
        for idx in self:
            del self[idx]


class MultiGraphSuccessorsMapping(GraphSuccessorsMapping):
    def __getitem__(self, nodeA):
        if not self.gorm._node_exists(self.graph.name, nodeA):
            raise KeyError("No such node")
        return self.Successors(self, nodeA)

    def __setitem__(self, nodeA, val):
        r = self.Successors(self, nodeA)
        r.clear()
        r.update(val)

    def __delitem__(self, nodeA):
        self.Successors(self, nodeA).clear()

    class Successors(GraphSuccessorsMapping.Successors):
        def _edges(self, nodeB):
            return MultiEdges(self.graph, self.nodeA, nodeB)

        def __getitem__(self, nodeB):
            r = self._edges(nodeB)
            if len(r) == 0:
                raise KeyError("No edge between these nodes")
            return r

        def __setitem__(self, nodeB, val):
            self._edges(nodeB).update(val)

        def __delitem__(self, nodeB):
            self._edges(nodeB).clear()


class MultiDiGraphPredecessorsMapping(DiGraphPredecessorsMapping):
    class Predecessors(DiGraphPredecessorsMapping.Predecessors):
        def _getsub(self, nodeA):
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

    def keys(self):
        return self.graph.keys

    def iterkeys(self):
        return self.graph.iterkeys()

    def itervalues(self):
        return self.graph.itervalues()

    def iteritems(self):
        return self.graph.iteritems()

    def values(self):
        return self.graph.values()


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
        try:
            del self.succ[u][v]
        except KeyError:
            raise NetworkXError("The edge {}-{} is not in the graph.".format(u, v))

    def remove_edges_from(self, ebunch):
        for e in ebunch:
            (u, v) = e[:2]
            if u in self.succ and v in self.succ[u]:
                del self.succ[u][v]


class MultiGraph(networkx.MultiGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self._name = name
        self.graph = GraphMapping(gorm, name)
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
        for e in ebunch:
            try:
                self.remove_edge(*e[:3])
            except NetworkXError:
                pass

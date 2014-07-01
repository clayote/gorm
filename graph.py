import networkx
from collections import MutableMapping
from record import (
    NodeRecord,
    NodeValRecord,
    EdgeRecord,
    EdgeValRecord,
    GraphValRecord
)



class GraphMapping(MutableMapping):
    """Mapping for graph attributes"""
    def __init__(self, graph):
        """Initialize private dict and store pointers to the graph and ORM"""
        self.graph = graph
        self.gorm = graph.gorm


    def __getitem__(self, key):
        rev = self.gorm.rev
        branches = tuple(self.gorm._active_branches())
        self.gorm.cursor.execute(
            "SELECT rev, value, type FROM graph_val WHERE "
            "graph=? AND "
            "key=? AND "
            "rev<=? AND "
            "branch IN ({qms});".format(
                qms=", ".join("?" * len(branches))
            ),
            (self.graph.name, key, rev) + branches
        )
        if self.gorm.cursor.rowcount <= 0:
            raise KeyError("No value for key")
        (rev, value, typ) = self.gorm.cursor.fetchone()
        for row in self.gorm.cursor:
            if row[0] > rev:
                (rev, value, typ) = row
        return self.gorm.cast_value(value, typ)

    def __setitem__(self, key, value):
        """Set key=value at the present branch and revision"""
        branch = self.gorm.branch
        rev = self.gorm.rev
        (strval, strtyp) = self.gorm.stringify_value(value)
        # delete first, in case there already is such an assignment
        self.gorm.cursor.execute(
            "DELETE FROM graph_val WHERE graph=? AND key=? AND branch=? AND rev=?;",
            (self.graph.name, key, branch, rev)
        )
        self.gorm.cursor.execute(
            "INSERT INTO graph_val (graph, key, branch, rev, value, type) VALUES "
            "(?, ?, ?, ?, ?, ?);",
            (self.graph.name, key, branch, rev, strval, strtyp)
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
            "INSERT INTO graph_val (graph, key, branch, rev, type) VALUES "
            "(?, ?, ?, ?, 'unset');",
            (self.graph.name, key, branch, rev)
        )

    def __iter__(self):
        rev = self.gorm.rev
        branches = tuple(self.gorm._active_branches())
        self.gorm.cursor.execute(
            "SELECT DISTINCT key, rev, type FROM graph_val WHERE "
            "graph=? AND "
            "rev<=? AND "
            "branch IN ({qms});".format(
                qms=", ".join("?" * len(branches))
            ),
            (self.graph.name, rev) + branches
        )
        d = {}
        for (key, rev, typ) in self.gorm.cursor:
            if key not in d or d[key][0] < rev:
                d[key] = (rev, typ)
        for (key, (rev, typ)) in d.iteritems():
            if typ != 'unset':
                yield key

    def __len__(self):
        n = 0
        for k in self:
            n += 1
        return n

    def clear(self):
        """Delete everything"""
        for k in self:
            del self[k]

    def __dict__(self):
        r = {}
        r.update(self)
        return r

    def __repr__(self):
        return repr(dict(self))


class GraphNodeMapping(GraphMapping):
    """Mapping for nodes in a graph"""
    class Node(GraphMapping):
        """Mapping for node attributes"""

        @property
        def exists(self):
            branches = tuple(self.gorm._active_branches())
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "SELECT nodes.extant FROM nodes JOIN ("
                "SELECT graph, node, branch, MAX(rev) AS rev "
                "FROM nodes WHERE "
                "graph=? AND "
                "node=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, node, branch) AS hirev "
                "ON nodes.graph=hirev.graph "
                "AND nodes.node=hirev.node "
                "AND nodes.branch=hirev.branch "
                "AND nodes.rev=hirev.rev;".format(
                    qms=", ".join("?" * len(branches))
                ), (
                    self.graph.name,
                    self.node,
                    rev
                ) + branches
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
            self.gorm.cursor.execute(
                "DELETE FROM nodes WHERE ("
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
            self.graph = graph
            self.gorm = graph.gorm
            self.node = node
            self.name = self.node

        def __getitem__(self, key):
            rev = self.gorm.rev
            branches = tuple(self.gorm._active_branches())
            self.gorm.cursor.execute(
                "SELECT node_val.value, node_val.type FROM node_val JOIN ("
                "SELECT graph, node, key, branch, MAX(rev) AS rev "
                "FROM node_val WHERE "
                "graph=? AND "
                "node=? AND "
                "key=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "AND type<>'unset' "
                "GROUP BY graph, node, key, branch) AS hirev "
                "ON node_val.graph=hirev.graph "
                "AND node_val.node=hirev.node "
                "AND node_val.key=hirev.key "
                "AND node_val.branch=hirev.branch "
                "AND node_val.rev=hirev.rev;".format(
                    qms=", ".join("?" * len(branches))
                ),
                (self.graph.name, self.node, key, rev) + branches
            )
            return self.gorm.cast_value(*self.gorm.cursor.fetchone())

        def __iter__(self):
            rev = self.gorm.rev
            branches = tuple(self.gorm._active_branches())
            self.gorm.cursor.execute(
                "SELECT DISTINCT node_val.key FROM node_val JOIN "
                "(SELECT graph, node, key, branch, MAX(rev) AS rev "
                "FROM node_val WHERE "
                "graph=? AND "
                "node=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, node, key, branch) AS hirev "
                "ON node_val.graph=hirev.graph "
                "AND node_val.node=hirev.node "
                "AND node_val.key=hirev.key "
                "AND node_val.branch=hirev.branch "
                "AND node_val.rev=hirev.rev "
                "WHERE node_val.type<>'unset';".format(
                    qms=", ".join("?" * len(branches))
                ), (
                    self.graph.name,
                    self.node,
                    rev
                ) + branches
            )
            for row in self.gorm.cursor:
                yield row[0]

        def __setitem__(self, key, value):
            """Set key=value at the present branch and revision. Overwrite if necessary."""
            branch = self.gorm.branch
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "DELETE FROM node_val WHERE graph=? AND node=? AND key=? AND branch=? AND rev=?;",
                (self.graph.name, self.node, key, branch, rev)
            )
            (val, typ) = self.gorm.stringify_value(value)
            self._del(key)
            self.gorm.cursor.execute(
                "INSERT INTO node_val (graph, node, key, branch, rev, value, type) VALUES "
                "(?, ?, ?, ?, ?, ?, ?);",
                (self.graph.name, self.node, key, branch, rev, val, typ)
            )

        def __delitem__(self, key):
            branch = self.gorm.branch
            rev = self.gorm.rev
            self.gorm.cursor.execute(
                "DELETE FROM node_val WHERE graph=? AND node=? AND key=? AND branch=? AND rev=?;",
                (self.graph.name, self.node, key, branch, rev)
            )
            self.gorm.cursor.execute(
                "INSERT INTO node_val (graph, node, key, branch, rev, type) VALUES "
                "(?, ?, ?, ?, ?, 'unset');",
                (self.graph.name, self.node, key, branch, rev)
            )

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
        for node in self.gorm._iternodes(self.graph):
            yield node

    def __len__(self):
        n = 0
        for node in iter(self):
            n += 1
        return n

    def __dict__(self):
        r = {}
        for (name, node) in self.iteritems():
            r[name] = dict(node)
        return r


class GraphEdgeMapping(GraphMapping):
    """Provides an adjacency mapping and possibly a predecessor mapping
    for a graph.

    """
    def __init__(self, graph):
        self.graph = graph
        self.gorm = graph.gorm

    def __iter__(self):
        for node in self.gorm._iternodes(self.graph):
            yield node

    def __len__(self):
        n = 0
        for nodeA in self:
            n += 1
        return n

    def __dict__(self):
        r = {}
        for (nodeA, succ) in self.iteritems():
            r[nodeA] = dict(succ)
        return r

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
                "SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
                "FROM edges WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "nodeB=? AND "
                "idx=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev "
                "ON edges.graph=hirev.graph "
                "AND edges.nodeA=hirev.nodeA "
                "AND edges.nodeB=hirev.nodeB "
                "AND edges.idx=hirev.idx "
                "AND edges.rev=hirev.rev;".format(
                    qms=", ".join("?" * len(branches))
                ), (
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.idx,
                    rev
                ) + branches
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
                    self.nodeA,
                    self.nodeB,
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
                    self.nodeA,
                    self.nodeB,
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
                "SELECT edge_val.value, edge_val.type FROM edge_val JOIN "
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
                "WHERE edge_val.type<>'unset';".format(
                    qms=", ".join("?" * len(branches))
                ),
                (
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.idx,
                    key,
                    rev
                ) + branches
            )
            return self.gorm.cast_value(*self.gorm.cursor.fetchone())

        def __iter__(self):
            rev = self.gorm.rev
            branches = tuple(self.gorm._active_branches())
            self.gorm.cursor.execute(
                "SELECT DISTINCT edge_val.key FROM edge_val JOIN ("
                "SELECT DISTINCT graph, nodeA, nodeB, idx, key, branch, MAX(rev) AS rev "
                "FROM edge_val WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "nodeB=? AND "
                "idx=? AND "
                "rev<=? AND "
                "branch IN ({qms})) AS hirev "
                "ON edge_val.graph=hirev.graph "
                "AND edge_val.nodeA=hirev.nodeA "
                "AND edge_val.nodeB=hirev.nodeB "
                "AND edge_val.idx=hirev.idx "
                "AND edge_val.rev=hirev.rev "
                "AND edge_val.key=hirev.key "
                "WHERE edge_val.type<>'unset';".format(
                    qms=", ".join("?" * len(branches))
                ), (
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.idx,
                    rev
                ) + branches
            )
            for row in self.gorm.cursor:
                yield row[0]

        def __setitem__(self, key, value):
            """Set a database record to say that key=value at the present branch
            and revision

            """
            branch = self.gorm.branch
            rev = self.gorm.rev
            (val, typ) = self.gorm.stringify_value(value)
            self.gorm.cursor.execute(
                "DELETE FROM edge_val WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "nodeB=? AND "
                "idx=? AND "
                "branch=? AND "
                "rev=?;",
                (
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.idx,
                    branch,
                    rev
                )
            )
            self.gorm.cursor.execute(
                "INSERT INTO edge_val (graph, nodeA, nodeB, idx, branch, rev, value, type) VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.idx,
                    branch,
                    rev,
                    val,
                    typ
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
                "branch=? AND "
                "rev=?;",
                (
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.idx,
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
                "branch, "
                "rev, "
                "type) VALUES (?, ?, ?, ?, ?, ?, 'unset');",
                (
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.idx,
                    branch,
                    rev
                )
            )


    class EdgeBackward(Edge):
        """Edge with the nodeA and nodeB reversed, for predecessor maps."""
        def __init__(self, graph, nodeB, nodeA, idx=0, existence=None, data=None):
            return super(GraphEdgeMapping.Edge, self).__init__(
                self,
                graph,
                nodeA,
                nodeB,
                idx,
                existence,
                data
            )


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
                "(SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
                "FROM edges WHERE "
                "graph=? AND "
                "nodeA=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev "
                "ON edges.graph=hirev.graph "
                "AND edges.nodeA=hirev.nodeA "
                "AND edges.nodeB=hirev.nodeB "
                "AND edges.idx=hirev.idx "
                "AND edges.rev=hirev.rev "
                "WHERE edges.extant={true};".format(
                    qms=", ".join("?" * len(branches)),
                    true=self.gorm.sql_types[self.gorm.sql_flavor]['true']
                ), (
                    self.graph.name,
                    self.nodeA,
                    rev
                ) + branches
            )
            for row in self.gorm.cursor:
                yield row[0]

        def __len__(self):
            n = 0
            for b in self:
                n += 1
            return n

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

        def __dict__(self):
            r = {}
            for (nodeB, edge) in self.iteritems():
                r[nodeB] = dict(edge)
            return r

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
                "(SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
                "FROM edges WHERE "
                "graph=? AND "
                "nodeB=? AND "
                "rev<=? AND "
                "branch IN ({qms}) "
                "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev "
                "ON edges.graph=hirev.graph "
                "AND edges.nodeA=hirev.nodeA "
                "AND edges.nodeB=hirev.nodeB "
                "AND edges.idx=hirev.idx "
                "AND edges.rev=hirev.rev "
                "WHERE edges.extant={true};".format(
                    qms=", ".join("?" * len(branches)),
                    true=self.gorm.sql_types[self.gorm.sql_flavor]['true']
                ), (
                    self.graph.name,
                    self.nodeB,
                    rev
                ) + branches
            )
            for row in self.gorm.cursor:
                yield row[0]

        def __len__(self):
            n = 0
            for a in self:
                n += 1
            return n

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
            "(SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
            "FROM edges WHERE "
            "graph=? AND "
            "nodeA=? AND "
            "nodeB=? AND "
            "rev<=? AND "
            "branch IN ({qms}) AND"
            "extant={true} "
            "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev "
            "ON edges.graph=hirev.graph "
            "AND edges.nodeA=hirev.nodeA "
            "AND edges.nodeB=hirev.nodeB "
            "AND edges.idx=hirev.idx "
            "AND edges.rev=hirev.rev;".format(
                qms=", ".join("?" * len(branches)),
                true=self.gorm.sql_types[self.gorm.sql_flavor]['true']
            ), (
                self.graph.name,
                self.nodeA,
                self.nodeB,
                rev
            ) + branches
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
            "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev "
            "ON edges.graph=hirev.graph "
            "AND edges.nodeA=hirev.nodeA "
            "AND edges.nodeB=hirev.nodeB "
            "AND edges.rev=hirev.rev "
            "WHERE edges.extant={true};".format(
                qms=", ".join("?" * len(branches)),
                true=self.gorm.sql_types[self.gorm.sql_flavor['true']]
            ), (
                self.graph.name,
                self.nodeA,
                self.nodeB,
                rev
            ) + branches
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

    def __dict__(self):
        r = {}
        for (idx, edge) in self.iteritems():
            r[idx] = dict(edge)
        return r

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


class MultiGraph(networkx.MultiGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self.graph = GraphMapping(gorm, name)
        self.node = GraphNodeMapping(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj


class MultiDiGraph(networkx.MultiDiGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self.graph = GraphMapping(gorm, name)
        self.node = GraphNodeMapping(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        self.pred = MultiDiGraphPredecessorsMapping(gorm, name)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj
 

# This file is part of gorm, an object relational mapper for versioned graphs.
# Copyright (C) 2014 Zachary Spector.
from .graph import (
    Graph,
    DiGraph,
    MultiGraph,
    MultiDiGraph,
    json_dump,
    json_load
)


class ORM(object):
    """Instantiate this with a database connector to use gorm."""
    sql_types = {
        'sqlite': {
            'text': 'TEXT',
            'integer': 'INTEGER',
            'boolean': 'BOOLEAN',
            'true': '1',
            'false': '0'
        }
    }
    """Important data types and values represented for different SQL
    flavors.

    """
    def __init__(
            self,
            connector,
            sql_flavor='sqlite',
            pickling=False,
            obranch=None,
            orev=None
    ):
        """Store connector and flags, and open a cursor"""
        self.pickling = pickling
        if sql_flavor not in self.sql_types:
            raise ValueError("Unknown SQL flavor")
        self.sql_flavor = sql_flavor
        self.connection = connector
        self.cursor = self.connection.cursor()
        self._obranch = obranch
        self._orev = orev
        self._branches = {}

    def __enter__(self):
        """Enable the use of the ``with`` keyword"""
        return self

    def __exit__(self, *args):
        """Alias for ``close``"""
        self.close()

    def _havebranch(self, b):
        """Private use. Checks that the branch is known about."""
        if b in self._branches:
            return True
        self.cursor.execute(
            "SELECT count(*) FROM branches WHERE branch=?;",
            (b,)
        )
        return self.cursor.fetchone()[0] == 1

    def is_parent_of(self, parent, child):
        """Return whether ``child`` is a branch descended from ``parent`` at
        any remove.

        """
        # trivial cases
        if child in self._branches and self._branches[child][0] == parent:
            return True
        elif child == parent:
            return False
        # I will be recursing a lot so just cache all the branch info
        self._childbranch = {}
        self._ancestry = {}
        for (branch, parent, parent_rev) in self.cursor.execute(
                "SELECT branch, parent, parent_rev FROM branches;"
        ).fetchall():
            self._branches[branch] = (parent, parent_rev)
            self._childbranch[parent] = branch

        self._ancestry[child] = set([parent])
        lineage = self._ancestry[child]

        def recurse(oneparent):
            if oneparent in lineage:
                return True
            if oneparent not in self._branches:
                return False
            if self._branches[oneparent][0] in lineage:
                return True
            lineage.add(oneparent)
            return recurse(self._branches[oneparent][0])
        return recurse(child)

    @property
    def branch(self):
        """Return the global value ``branch``, or ``self._obranch`` if it's
        set"""
        if self._obranch is not None:
            return self._obranch
        self.cursor.execute(
            "SELECT value FROM global WHERE key=?;",
            (
                json_dump('branch'),
            )
        )
        return json_load(self.cursor.fetchone()[0])

    @branch.setter
    def branch(self, v):
        """Set the global value ``branch`` and note that the branch's (parent,
        parent_rev) are the (branch, tick) set previously

        """
        curbranch = self.branch
        currev = self.rev
        if not self._havebranch(v):
            # assumes the present revision in the parent branch has
            # been finalized.
            self.cursor.execute(
                "INSERT INTO branches (branch, parent, parent_rev) "
                "VALUES (?, ?, ?);",
                (json_dump(v), curbranch, currev)
            )
        if v == 'master':
            return
        # make sure I'll end up within the revision range of the
        # destination branch
        self.cursor.execute(
            "SELECT parent_rev FROM branches WHERE branch=?;",
            (v,)
        )
        parrev = self.cursor.fetchone()[0]
        if currev < parrev:
            raise ValueError(
                "Tried to jump to branch {br}, which starts at revision {rv}. "
                "Go to rev {rv} or later to use this branch.".format(
                    br=v,
                    rv=currev
                )
            )
        self.cursor.execute(
            "UPDATE global SET value=? WHERE key='branch';",
            (json_dump(v),)
        )

    @property
    def rev(self):
        """Return the global value ``rev``, or ``self._orev`` if that's set"""
        if self._orev is not None:
            return self._orev
        self.cursor.execute(
            "SELECT value FROM global WHERE key=?;",
            (
                json_dump('rev'),
            )
        )
        return json_load(self.cursor.fetchone()[0])

    @rev.setter
    def rev(self, v):
        """Set the global value ``rev``, first checking that it's not before
        the start of this branch. If it is, also go to the parent
        branch.

        """
        # first make sure the cursor is not before the start of this branch
        branch = self.branch
        if branch != 'master':
            self.cursor.execute(
                "SELECT parent, parent_rev FROM branches WHERE branch=?;",
                (json_dump(branch),)
            )
            (parent, parent_rev) = self.cursor.fetchone()
            if v < int(parent_rev):
                raise ValueError(
                    "The revision number {revn} "
                    "occurs before the start of "
                    "the branch {brnch}".format(revn=v, brnch=branch)
                )
        self.cursor.execute(
            "UPDATE global SET value=? WHERE key=?;",
            (
                json_dump(v),
                json_dump('rev')
            )
        )
        assert(self.rev == v)

    def close(self):
        """Commit the transaction and close the cursor.

        Don't close the connection--I don't know what else is to be
        done with it.

        """
        # maybe these should be in the opposite order?
        self.connection.commit()
        self.cursor.close()

    def initdb(self):
        """Create the database schema that I use, and put the (branch, rev)
        cursor at ('master', 0).

        """
        tabdecls = [
            "CREATE TABLE global ("
            "key {text} NOT NULL PRIMARY KEY, "
            "value {text})",
            ";",
            "CREATE TABLE branches ("
            "branch {text} NOT NULL DEFAULT 'master', "
            "parent {text} NOT NULL DEFAULT 'master', "
            "parent_rev {integer} NOT NULL DEFAULT 0, "
            "PRIMARY KEY(branch), "
            "FOREIGN KEY(parent) REFERENCES branch(branch)"
            ");",
            "CREATE TABLE graphs ("
            "graph {text} NOT NULL, "
            "type {text} NOT NULL DEFAULT 'Graph', "
            "PRIMARY KEY(graph), "
            "CHECK(type IN ('Graph', 'DiGraph', 'MultiGraph', 'MultiDiGraph'))"
            ");",
            "INSERT INTO branches DEFAULT VALUES;",
            "CREATE TABLE graph_val ("
            "graph {text} NOT NULL, "
            "key {text} NOT NULL, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "value {text}, "
            "PRIMARY KEY (graph, key, branch, rev), "
            "FOREIGN KEY(graph) REFERENCES graphs(graph), "
            "FOREIGN KEY(branch) REFERENCES branches(branch))"
            ";",
            "CREATE INDEX graph_val_idx ON graph_val(graph, key)"
            ";",
            "CREATE TABLE nodes ("
            "graph {text} NOT NULL, "
            "node {text} NOT NULL, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "extant {boolean} NOT NULL, "
            "PRIMARY KEY (graph, node, branch, rev), "
            "FOREIGN KEY(graph) REFERENCES graphs(graph), "
            "FOREIGN KEY(branch) REFERENCES branches(branch))"
            ";",
            "CREATE INDEX nodes_idx ON nodes(graph, node)"
            ";",
            "CREATE TABLE node_val ("
            "graph {text} NOT NULL, "
            "node {text} NOT NULL, "
            "key {text} NOT NULL, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "value {text}, "
            "PRIMARY KEY(graph, node, key, branch, rev), "
            "FOREIGN KEY(graph, node) REFERENCES nodes(graph, node), "
            "FOREIGN KEY(branch) REFERENCES branches(branch))"
            ";",
            "CREATE INDEX node_val_idx ON node_val(graph, node, key)"
            ";",
            "CREATE TABLE edges ("
            "graph {text} NOT NULL, "
            "nodeA {text} NOT NULL, "
            "nodeB {text} NOT NULL, "
            "idx {integer} NOT NULL DEFAULT 0, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "extant {boolean} NOT NULL, "
            "PRIMARY KEY (graph, nodeA, nodeB, idx, branch, rev), "
            "FOREIGN KEY(graph, nodeA) REFERENCES nodes(graph, node), "
            "FOREIGN KEY(graph, nodeB) REFERENCES nodes(graph, node), "
            "FOREIGN KEY(branch) REFERENCES branches(branch))"
            ";",
            "CREATE INDEX edges_idx ON edges(graph, nodeA, nodeB, idx)"
            ";",
            "CREATE TABLE edge_val ("
            "graph {text} NOT NULL, "
            "nodeA {text} NOT NULL, "
            "nodeB {text} NOT NULL, "
            "idx {integer} NOT NULL DEFAULT 0, "
            "key {text}, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "value {text}, "
            "PRIMARY KEY(graph, nodeA, nodeB, idx, key, branch, rev), "
            "FOREIGN KEY(graph, nodeA, nodeB, idx) "
            "REFERENCES edges(graph, nodeA, nodeB, idx), "
            "FOREIGN KEY(branch) REFERENCES branches(branch))"
            ";",
            "CREATE INDEX edge_val_idx ON edge_val(graph, nodeA, nodeB, idx, key)"
            ";"
        ]
        for decl in tabdecls:
            s = decl.format(**self.sql_types[self.sql_flavor])
            self.cursor.execute(s)
        globs = [
            ("branch", "master"),
            ("rev", 0)
        ]
        self.cursor.executemany(
            "INSERT INTO global (key, value) VALUES (?, ?);",
            (
                (json_dump(glob[0]), json_dump(glob[1]))
                for glob in globs
            )
        )

    def _init_graph(self, name, type_s='Graph'):
        if self.cursor.execute(
            "SELECT COUNT(*) FROM graphs WHERE graph=?;",
            (name,)
        ).fetchone()[0]:
            raise KeyError("Already have a graph by that name")
        self.cursor.execute(
            "INSERT INTO graphs (graph, type) VALUES (?, ?);",
            (name, type_s)
        )

    def new_graph(self, name, data=None, **attr):
        """Return a new instance of type Graph, initialized with the given
        data if provided.

        """
        self._init_graph(json_dump(name), 'Graph')
        return Graph(self, name, data, **attr)

    def new_digraph(self, name, data=None, **attr):
        """Return a new instance of type DiGraph, initialized with the given
        data if provided.

        """
        self._init_graph(json_dump(name), 'DiGraph')
        return DiGraph(self, name, data, **attr)

    def new_multigraph(self, name, data=None, **attr):
        """Return a new instance of type MultiGraph, initialized with the given
        data if provided.

        """
        self._init_graph(json_dump(name), 'MultiGraph')
        return MultiGraph(self, name, data, **attr)

    def new_multidigraph(self, name, data=None, **attr):
        """Return a new instance of type MultiDiGraph, initialized with the given
        data if provided.

        """
        self._init_graph(name, 'MultiDiGraph')
        return MultiDiGraph(self, json_dump(name), data, **attr)

    def get_graph(self, name):
        """Return a graph previously created with ``new_graph``,
        ``new_digraph``, ``new_multigraph``, or
        ``new_multidigraph``

        """
        n = json_dump(name)
        self.cursor.execute("SELECT type FROM graphs WHERE graph=?;", (n,))
        try:
            (type_s,) = self.cursor.fetchone()
        except TypeError:
            raise ValueError("I don't know of a graph named {}".format(n))
        return {
            'Graph': Graph,
            'DiGraph': DiGraph,
            'MultiGraph': MultiGraph,
            'MultiDiGraph': MultiDiGraph
        }[type_s](self, name)

    def del_graph(self, name):
        """Remove all traces of a graph's existence from the database"""
        # make sure the graph exists before deleting anything
        self.get_graph(name)
        n = json_dump(name)
        for statement in [
                "DELETE FROM edge_val WHERE graph=?;",
                "DELETE FROM edges WHERE graph=?;",
                "DELETE FROM node_val WHERE graph=?;",
                "DELETE FROM nodes WHERE graph=?;",
                "DELETE FROM graphs WHERE graph=?;"
        ]:
            self.cursor.execute(statement, (n,))

    def _active_branches(self):
        """Private use. Iterate over (branch, rev) pairs, where the branch is
        a descendant of the previous (ending at 'master'), and the rev
        is the latest revision in the branch that matters.

        """
        branch = self.branch
        rev = self.rev
        yield (branch, rev)
        while branch != 'master':
            if branch not in self._branches:
                self._branches[branch] = self.cursor.execute(
                    "SELECT parent, parent_rev FROM branches WHERE branch=?;",
                    (branch,)
                ).fetchone()
            (branch, rev) = self._branches[branch]
            yield (branch, rev)

    def _iternodes(self, graphn):
        """Iterate over all nodes that presently exist in the graph"""
        seen = set()
        graph = json_dump(graphn)
        for (branch, rev) in self._active_branches():
            data = self.cursor.execute(
                "SELECT nodes.node "
                "FROM nodes JOIN ("
                "SELECT graph, node, branch, MAX(rev) AS rev FROM nodes "
                "WHERE graph=? "
                "AND branch=? "
                "AND rev<=? "
                "GROUP BY graph, node, branch) AS hirev "
                "ON nodes.graph=hirev.graph "
                "AND nodes.node=hirev.node "
                "AND nodes.branch=hirev.branch "
                "AND nodes.rev=hirev.rev "
                "WHERE nodes.node IS NOT NULL "
                "AND nodes.extant;",
                (
                    graph,
                    branch,
                    rev
                )
            ).fetchall()
            for row in data:
                node = json_load(row[0])
                if node not in seen:
                    yield node
                seen.add(node)

    def _countnodes(self, graphn):
        """How many nodes presently exist in the graph?"""
        n = 0
        for node in self._iternodes(graphn):
            n += 1
        return n

    def _node_exists(self, graphn, node):
        """Does this node presently exist in this graph?"""
        n = json_dump(node)
        graph = json_dump(graphn)
        for (branch, rev) in self._active_branches():
            self.cursor.execute(
                "SELECT nodes.extant FROM nodes JOIN ("
                "SELECT graph, node, branch, MAX(rev) AS rev FROM nodes "
                "WHERE graph=? "
                "AND node=? "
                "AND branch=? "
                "AND rev<=? "
                "GROUP BY graph, node, branch) AS hirev "
                "ON nodes.graph=hirev.graph "
                "AND nodes.node=hirev.node "
                "AND nodes.branch=hirev.branch "
                "AND nodes.rev=hirev.rev;",
                (
                    graph,
                    n,
                    branch,
                    rev
                )
            )
            data = self.cursor.fetchall()
            if len(data) == 0:
                continue
            elif len(data) > 1:
                raise ValueError("Silly data in nodes table")
            else:
                return bool(data.pop()[0])
        return False

# This file is part of gorm, an object relational mapper for versioned graphs.
# Copyright (C) 2014 Zachary Spector.
from pickle import Pickler, Unpickler
from json import dumps as jsonned
from json import loads as unjsonned
from StringIO import StringIO
from graph import (
    Graph,
    DiGraph,
    MultiGraph,
    MultiDiGraph
)


def pickled(v):
    """Return a string representation of ``v`` pickled.

    Uses pickle protocol version 0.

    """
    io = StringIO()
    pck = Pickler(io)
    pck.dump(v)
    r = io.getvalue()
    io.close()
    return r


def unpickled(s):
    """Take a pickled string representation of an object and return the
    object.

    Uses pickle protocol version 0.

    """
    io = StringIO(s)
    upck = Unpickler(io)
    r = upck.load()
    io.close()
    return r


class ORM(object):
    """Instantiate this with a database connector to use gorm."""
    str2type = {
        'bool': bool,
        'int': int,
        'float': float,
        'str': str,
        'unicode': unicode
    }
    """Map string names of primitive types to the types themselves."""
    type2str = {
        bool: 'bool',
        int: 'int',
        float: 'float',
        str: 'str',
        unicode: 'unicode'
    }
    """Map types to their string representations."""
    sql_types = {
        'sqlite': {
            'text': 'TEXT',
            'integer': 'INTEGER',
            'boolean': 'BOOLEAN',
            'true': '1',
            'false': '0'
        }
    }
    """Important data types and values represented for different SQL flavors."""
    def __init__(
            self,
            connector,
            sql_flavor='sqlite',
            pickling=False
    ):
        """Store connector and flags, and open a cursor"""
        self.pickling = pickling
        if sql_flavor not in self.sql_types:
            raise ValueError("Unknown SQL flavor")
        self.sql_flavor = sql_flavor
        self.connection = connector
        self.cursor = self.connection.cursor()

    def __enter__(self):
        """Enable the use of the ``with`` keyword"""
        return self

    def __exit__(self, *args):
        """Alias for ``close``"""
        self.close()

    def _havebranch(self, b):
        """Private use. Checks that the branch is known about."""
        self.cursor.execute(
            "SELECT count(*) FROM branches WHERE branch=?;",
            (b,)
        )
        return self.cursor.fetchone()[0] == 1

    @property
    def branch(self):
        self.cursor.execute(
            "SELECT value FROM global WHERE key='branch';"
        )
        return self.cursor.fetchone()[0]

    @branch.setter
    def branch(self, v):
        curbranch = self.branch
        currev = self.rev
        if not self._havebranch(v):
            # assumes the present revision in the parent branch has
            # been finalized.
            self.cursor.execute(
                "INSERT INTO branches (branch, parent, parent_rev) "
                "VALUES (?, ?, ?);",
                (v, curbranch, currev)
            )
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
            (v,)
        )

    @property
    def rev(self):
        self.cursor.execute(
            "SELECT value FROM global WHERE key='rev';"
        )
        return int(self.cursor.fetchone()[0])

    @rev.setter
    def rev(self, v):
        # first make sure the cursor is not before the start of this branch
        branch = self.branch
        self.cursor.execute(
            "SELECT parent, parent_rev FROM branches WHERE branch=?;",
            (branch,)
        )
        (parent, parent_rev) = self.cursor.fetchone()
        if v < int(parent_rev):
            raise ValueError(
                "The revision number {revn} "
                "occurs before the start of "
                "the branch {brnch}".format(revn=v, brnch=branch)
            )
        self.cursor.execute(
            "UPDATE global SET value=? WHERE key='rev';",
            (v,)
        )

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
            "key {text} NOT NULL, "
            "value {text}, "
            "type {text} NOT NULL, "
            "PRIMARY KEY (key), "
            "CHECK(type IN "
            "('pickle', 'json', 'str', 'unicode', 'int', 'float', 'bool', 'unset'))"
            ");",
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
            "valtype {text} NOT NULL, "
            "PRIMARY KEY (graph, key, branch, rev), "
            "FOREIGN KEY(graph) REFERENCES graphs(graph), "
            "FOREIGN KEY(branch) REFERENCES branches(branch), "
            "CHECK(valtype IN "
            "('pickle', 'json', 'str', 'unicode', 'int', 'float', 'bool', 'unset'))"
            ");",
            "CREATE TABLE nodes ("
            "graph {text} NOT NULL, "
            "node {text} NOT NULL, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "extant {boolean} NOT NULL, "
            "PRIMARY KEY (graph, node, branch, rev), "
            "FOREIGN KEY(graph) REFERENCES graphs(graph), "
            "FOREIGN KEY(branch) REFERENCES branches(branch));",
            "CREATE TABLE node_val ("
            "graph {text} NOT NULL, "
            "node {text} NOT NULL, "
            "key {text} NOT NULL, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "value {text}, "
            "valtype {text} NOT NULL, "
            "PRIMARY KEY(graph, node, key, branch, rev), "
            "FOREIGN KEY(graph, node) REFERENCES nodes(graph, node), "
            "FOREIGN KEY(branch) REFERENCES branches(branch), "
            "CHECK(valtype IN "
            "('pickle', 'json', 'str', 'unicode', 'int', 'float', 'bool', 'unset'))"
            ");",
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
            "FOREIGN KEY(branch) REFERENCES branches(branch)"
            ");",
            "CREATE TABLE edge_val ("
            "graph {text} NOT NULL, "
            "nodeA {text} NOT NULL, "
            "nodeB {text} NOT NULL, "
            "idx {integer} NOT NULL DEFAULT 0, "
            "key {text}, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "value {text}, "
            "valtype {text} NOT NULL, "
            "PRIMARY KEY(graph, nodeA, nodeB, idx, key, branch, rev), "
            "FOREIGN KEY(graph, nodeA, nodeB, idx) "
            "REFERENCES edges(graph, nodeA, nodeB, idx), "
            "FOREIGN KEY(branch) REFERENCES branches(branch), "
            "CHECK(valtype IN "
            "('pickle', 'json', 'str', 'unicode', 'int', 'float', 'bool', 'unset'))"
            ");"
        ]
        for decl in tabdecls:
            s = decl.format(**self.sql_types[self.sql_flavor])
            self.cursor.execute(s)
        globs = [
            ("branch", "master", "str"),
            ("rev", 0, "int")
        ]
        self.cursor.executemany(
            "INSERT INTO global (key, value, type) VALUES (?, ?, ?);",
            globs
        )

    def cast(self, value, typestr):
        """Return ``value`` cast into the type indicated by ``typestr``"""
        if typestr == 'pickle':
            if self.pickling:
                return unpickled(value)
            else:
                raise TypeError(
                    "This value is pickled, but pickling is disabled"
                )
        elif typestr == 'json':
            return unjsonned(value)
        else:
            return self.str2type[typestr](value)

    def stringify(self, value):
        """Return a pair of a string representing the value, and another
        string describing its type (for use with ``cast_value``)

        """
        if type(value) in self.type2str:
            return (value, self.type2str[type(value)])
        try:
            return (jsonned(value), 'json')
        except TypeError:
            if self.pickling:
                return (pickled(value), 'pickle')
            else:
                raise TypeError(
                    "Value isn't serializable without pickling"
                )

    def _init_graph(self, name, type_s='Graph'):
        self.cursor.execute(
            "INSERT INTO graphs (graph, type) VALUES (?, ?);",
            (name, type_s)
        )

    def new_graph(self, name, data=None, **attr):
        """Return a new instance of type Graph, initialized with the given
        data if provided.

        """
        self._init_graph(name, 'Graph')
        return Graph(self, name, data, **attr)

    def new_digraph(self, name, data=None, **attr):
        """Return a new instance of type DiGraph, initialized with the given
        data if provided.

        """
        self._init_graph(name, 'DiGraph')
        return DiGraph(self, name, data, **attr)

    def new_multigraph(self, name, data=None, **attr):
        """Return a new instance of type MultiGraph, initialized with the given
        data if provided.

        """
        self._init_graph(name, 'MultiGraph')
        return MultiGraph(self, name, data, **attr)

    def new_multidigraph(self, name, data=None, **attr):
        """Return a new instance of type MultiDiGraph, initialized with the given
        data if provided.

        """
        self._init_graph(name, 'MultiDiGraph')
        return MultiDiGraph(self, name, data, **attr)

    def get_graph(self, name):
        """Return a graph previously created with ``new_graph``,
        ``new_digraph``, ``new_multigraph``, or
        ``new_multidigraph``

        """
        self.cursor.execute("SELECT type FROM graphs WHERE graph=?;", (name,))
        try:
            (type_s,) = self.cursor.fetchone()
        except TypeError:
            raise ValueError("I don't know of a graph named {}".format(name))
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
        for statement in [
                "DELETE FROM edge_val WHERE graph=?;",
                "DELETE FROM edges WHERE graph=?;",
                "DELETE FROM node_val WHERE graph=?;",
                "DELETE FROM nodes WHERE graph=?;",
                "DELETE FROM graphs WHERE graph=?;"
        ]:
            self.cursor.execute(statement, (name,))

    def _active_branches(self):
        """Private use. Iterate over (branch, rev) pairs, where the branch is
        a descendant of the previous (ending at 'master'), and the rev
        is the latest revision in the branch that matters.

        """
        branch = self.branch
        rev = self.rev
        yield (branch, rev)
        while branch != 'master':
            self.cursor.execute(
                "SELECT parent, parent_rev FROM branches WHERE branch=?;",
                (branch,)
            )
            (branch, rev) = self.cursor.fetchone()
            yield (branch, rev)

    def _iternodes(self, graph):
        """Iterate over all nodes that presently exist in the graph"""
        seen = set()
        for (branch, rev) in self._active_branches():
            self.cursor.execute(
                "SELECT nodes.node, nodes.extant "
                "FROM nodes JOIN ("
                "SELECT graph, node, branch, MAX(rev) AS rev FROM nodes "
                "WHERE graph=? "
                "AND branch=? "
                "AND rev<=? "
                "GROUP BY graph, node, branch) AS hirev "
                "ON nodes.graph=hirev.graph "
                "AND nodes.node=hirev.node "
                "AND nodes.branch=hirev.branch "
                "AND nodes.rev=hirev.rev;",
                (
                    graph,
                    branch,
                    rev
                )
            )
            data = self.cursor.fetchall()
            for row in data:
                try:
                    node = int(row[0])
                except ValueError:
                    node = row[1]
                if node in seen:
                    continue
                seen.add(node)
                extant = bool(row[1])
                if extant:
                    yield node

    def _countnodes(self, graph):
        """How many nodes presently exist in the graph?"""
        n = 0
        for node in self._iternodes(graph):
            n += 1
        return n

    def _node_exists(self, graph, node):
        """Does this node presently exist in this graph?"""
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
                    node,
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

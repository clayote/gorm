from pickle import Pickler, Unpickler
from StringIO import StringIO
from record import EdgeValRecord
from graph import (
    Graph,
    DiGraph,
    MultiGraph,
    MultiDiGraph
)


sql_types = {
    'sqlite': {
        'text': 'TEXT',
        'integer': 'INTEGER',
    }
}


def pickled(v):
    io = StringIO()
    pck = Pickler(io)
    pck.dump(v)
    r = io.getvalue()
    io.close()
    return r


def unpickled(s):
    io = StringIO(s)
    upck = Unpickler(io)
    r = upck.load()
    io.close()
    return r


class ORM(object):
    str2type = {
        'bool': bool,
        'int': int,
        'float': float,
        'str': str,
        'unicode': unicode
    }
    type2str = {
        bool: 'bool',
        int: 'int',
        float: 'float',
        str: 'str',
        unicode: 'unicode'
    }
    def __init__(
            self,
            branch='master',
            rev=0,
            connector=None,
            cache=None,
            sql_flavor='sqlite',
            pickling=False
    ):
        self.pickling = pickling
        if sql_flavor not in sql_types:
            raise ValueError("Unknown SQL flavor")
        self.sql_flavor = sql_flavor
        if connector is None:
            from sqlite3 import connect
            self.connector = connect(':memory:')
        else:
            self.connector = connector
        if cache is None:
            self.cache = {
                'graphs': {},
                'branches': {},
                'branch': branch,
                'rev': rev
            }
        else:
            self.cache = cache
        self.cursor = self.connector.cursor()
        try:
            self.fetch_globals()
        except:
            # What exceptions to catch depends on what SQL
            # flavor. I'm not presently literate enough about
            # database flavors to tell what I need to handle
            # exactly, so I'm just going to assume the database
            # isn't initialized. Revisit soon.
            self.initdb()
            self.fetch_globals()

    def __exit__(self):
        self.close()

    def writerec(self, rec):
        """Write the record into the SQL database, first deleting any existing
        record with a matching key.

        """
        self.cursor.execute(rec.sql_del, rec.key)
        self.cursor.execute(rec.sql_ins, rec)

    def close(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def fetch_globals(self):
        self.cursor.execute(
            "SELECT key, value FROM global;"
        )
        for (k, v) in self.cursor:
            self.cache[k] = v

    def initdb(self):
        tabdecls = [
            "CREATE TABLE global ("
            "key {text} NOT NULL, "
            "value {text} NOT NULL, "
            "type {text} NOT NULL, "
            "PRIMARY KEY (key), "
            "CHECK(type IN "
            "('pickle', 'str', 'unicode', 'int', 'float', 'bool'))"
            ");",
            "CREATE TABLE graphs ("
            "graph {text} NOT NULL, "
            "type {text} NOT NULL DEFAULT 'Graph', "
            "PRIMARY KEY(graph), "
            "CHECK(type IN ('Graph', 'DiGraph', 'MultiGraph', 'MultiDiGraph'))"
            ");",
            "CREATE TABLE branches ("
            "branch {text} NOT NULL DEFAULT 'master', "
            "parent {text} NOT NULL DEFAULT 'master', "
            "parent_rev {integer} NOT NULL DEFAULT 0, "
            "PRIMARY KEY(branch), "
            "FOREIGN KEY(parent) REFERENCES branch(branch)"
            ");",
            "CREATE TABLE nodes ("
            "graph {text} NOT NULL, "
            "node {text} NOT NULL, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "exists {boolean} NOT NULL, "
            "PRIMARY KEY (graph, node, branch, rev), "
            "FOREIGN KEY(graph) REFERENCES graphs(graph)"
            ");"
            "CREATE TABLE node_val ("
            "graph {text} NOT NULL, "
            "node {text} NOT NULL, "
            "key {text} NOT NULL, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "value {text}, "
            "type {text} NOT NULL, "
            "PRIMARY KEY(graph, node, key, branch, rev), "
            "FOREIGN KEY(graph, node) REFERENCES nodes(graph, node), "
            "CHECK(type IN "
            "('pickle', 'str', 'unicode', 'int', 'float', 'bool'))"
            ");",
            "CREATE TABLE edges ("
            "graph {text} NOT NULL, "
            "nodeA {text} NOT NULL, "
            "nodeB {text} NOT NULL, "
            "idx {integer} NOT NULL DEFAULT 0, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "exists {boolean} NOT NULL, "
            "PRIMARY KEY (graph, nodeA, nodeB, idx, branch, rev), "
            "FOREIGN KEY(graph, nodeA) REFERENCES nodes(graph, node), "
            "FOREIGN KEY(graph, nodeB) REFERENCES nodes(graph, node)"
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
            "type {text} NOT NULL, "
            "PRIMARY KEY(graph, nodeA, nodeB, idx, key, branch, rev), "
            "FOREIGN KEY(graph, nodeA, nodeB, idx) "
            "REFERENCES edges(graph, nodeA, nodeB, idx), "
            "CHECK(type IN "
            "('pickle', 'str', 'unicode', 'int', 'float', 'bool'))"
            ");"
        ]
        for decl in tabdecls:
            s = decl.format(**sql_types[self.sql_flavor])
            print(s)
            self.cursor.execute(s)
        globs = [
            ("branch", "master", "str"),
            ("tick", 0, "int")
        ]
        self.cursor.executemany(
            "INSERT INTO global (key, value, type) VALUES (?, ?, ?);",
            globs
        )

    def load_graph(self, graph):
        self.load_graph_branch(graph, self.cache['branch'])

    def load_graph_branch(self, graph, branch):
        pass

    def cast_value(self, value, typestr):
        """Return ``value`` cast into the type indicated by ``typestr``"""
        if typestr == 'pickle':
            if self.pickling:
                return unpickled(value)
            else:
                raise TypeError(
                    "This value is pickled, but pickling is disabled"
                )
        else:
            return self.str2type[typestr](value)

    def stringify_value(self, value):
        """Return a pair of a string representing the value, and another
        string describing its type (for use with ``cast_value``)

        """
        if type(value) in self.type2str:
            return (value, self.type2str[type(value)])
        elif self.pickling:
            return (pickled(value), 'pickle')
        else:
            raise TypeError(
                "Value isn't primitive, and I won't "
                "pickle it because you have pickling disabled."
            )

    def new_graph(self, name, type_s='Graph', data=None, **attr):
        if type_s not in (
                'Graph',
                'DiGraph',
                'MultiGraph',
                'MultiDiGraph'
        ):
            raise ValueError(
                "Acceptable type strings: 'Graph', 'DiGraph', 'MultiGraph', 'MultiDiGraph'"
            )
        self.cursor.execute(
            "INSERT INTO graph (graph, type) VALUES (?, ?);",
            (name, type_s)
        )
        return {
            'Graph': Graph,
            'DiGraph': DiGraph,
            'MultiGraph': MultiGraph,
            'MultiDiGraph': MultiDiGraph
        }[type_s](self, name, data, **attr)

    def get_graph(self, name):
        self.cursor.execute("SELECT type FROM graph WHERE name=?;", (name,))
        (type_s,) = self.cursor.fetchone()
        return {
            'Graph': Graph,
            'DiGraph': DiGraph,
            'MultiGraph': MultiGraph,
            'MultiDiGraph': MultiDiGraph
        }[type_s](self, name)

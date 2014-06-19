from util import value_during
from pickle import Pickler, Unpickler
from StringIO import StringIO
from record import EdgeRecord
from graph import (
    Graph,
    MultiGraph
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


class Gorm(object):
    def __init__(
            self,
            connector=None,
            cache=None,
            sql_flavor='sqlite',
            disable_pickling=False
    ):
        self.pickling = not disable_pickling
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
                'node_val': {},
                'edge_val': {},
                'graph_val': {},
                'graphs': {}
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
            "CREATE TABLE graph ("
            "graph {text} NOT NULL, "
            "type {text} NOT NULL DEFAULT 'Graph', "
            "PRIMARY KEY(graph), "
            "CHECK(type IN 'Graph', 'DiGraph', 'MultiGraph', 'MultiDiGraph')"
            ");",
            "CREATE TABLE branch ("
            "branch {text} NOT NULL DEFAULT 'master', "
            "parent {text} NOT NULL DEFAULT 'master', "
            "parent_rev {integer} NOT NULL DEFAULT 0, "
            "PRIMARY KEY(branch), "
            "FOREIGN KEY parent REFERENCES branch(branch)"
            ");",
            "CREATE TABLE node_val ("
            "graph {text} NOT NULL, "
            "node {text} NOT NULL, "
            "key {text}, "
            "branch {text} NOT NULL DEFAULT 'master', "
            "rev {integer} NOT NULL DEFAULT 0, "
            "value {text}, "
            "type {text} NOT NULL, "
            "PRIMARY KEY(graph, node, key, branch, rev),"
            "FOREIGN KEY(graph) REFERENCES graph(graph), "
            "CHECK(type IN "
            "('pickle', 'str', 'unicode', 'int', 'float', 'bool'))"
            ");",
            # The value for the null key is set to 1 when the node
            # exists, and 0 when it doesn't. Setting any key's value
            # to None (ie. null) is equivalent to deleting the key at
            # that revision.
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
            "CHECK(type IN "
            "('pickle', 'str', 'unicode', 'int', 'float', 'bool'))"
            ");"
            # The existence of a portal implies the existence of its
            # endpoints, even if one or both of them have been
            # previously declared not to exist. They do now.
        ]
        for decl in tabdecls:
            self.cursor.execute(decl.format(**sql_types[self.sql_flavor]))
        globs = [
            ("branch", "master"),
            ("tick", 0)
        ]
        self.cursor.executemany(
            "INSERT INTO global VALUES (?, ?);",
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
            return {
                'bool': bool,
                'int': int,
                'float': float,
                'str': str,
                'unicode': unicode
            }[typestr](value)

    def stringify_value(self, value):
        """Return a pair of a string representing the value, and another
        string describing its type (for use with ``cast_value``)

        """
        d = {
            bool: 'bool',
            int: 'int',
            float: 'float',
            str: 'str',
            unicode: 'unicode'
        }
        if type(value) in d:
            return (value, d[type(value)])
        elif self.pickling:
            return (pickled(value), 'pickle')
        else:
            raise TypeError(
                "Value isn't primitive, and I won't "
                "pickle it because you have pickling disabled."
            )

    def value_during(self, keys, branch, rev):
        def value_during_recurse(skel, branch, rev):
            if branch not in self._branches_loaded:
                self.load_graph_branch(keys[1], branch)
            if branch in skel:
                if rev in skel[branch]:
                    return skel[branch][rev].value
                return skel[branch][max(
                    r for r in skel[branch]
                    if r < rev
                )].value
            elif branch == "master":
                raise KeyError("Could not find value")
            else:
                tree = self.cache['branch_tree'][branch]
                return value_during_recurse(
                    skel,
                    tree.parent,
                    tree.rev - 1
                )
        skel = self.cache[keys.pop(0)]
        while keys:
            skel = skel[keys.pop(0)]
        return value_during_recurse(skel, branch, rev)

    def current_value(self, *keys):
        return self.value_during(
            keys,
            self.cache['branch'],
            self.cache['rev']
        )

    def node_exists(self, graph, node):
        branch = self.cache['branch']
        rev = self.cache['rev']
        if graph not in self.cache['node_val']:
            raise KeyError("No such graph")
        if node not in self.cache['node_val'][graph]:
            return False
        skel = self.cache['node_val'][graph][node][None]
        return value_during(skel, self.cache['branch_tree'], branch, rev)

    def edge_exists(self, graph, nodeA, nodeB, idx=0):
        branch = self.cache['branch']
        rev = self.cache['rev']
        if graph not in self.cache['edge_val']:
            raise KeyError("No such graph")
        if not (
                self.node_exists(graph, nodeA) and
                self.node_exists(graph, nodeB)
        ):
            return False
        if (
                nodeA not in self.cache['edge_val'][graph] or
                nodeB not in self.cache['edge_val'][graph][nodeA] or
                idx not in self.cache['edge_val'][graph][nodeA][nodeB]
        ):
            return False
        skel = self.cache['edge_val'][graph][nodeA][nodeB][idx][None]
        return value_during(skel, self.cache['branch_tree'], branch, rev)

    def set_record(self, rec, do_write=True, reciprocate=True):
        keys = list(rec.keynames)
        finalkey = keys.pop()
        skel = self.cache
        while keys:
            k = keys.pop(0)
            if k not in skel:
                skel[k] = {}
            skel = skel[k]
        skel[finalkey] = rec
        if do_write:
            self.cursor.execute(rec.sql_del, rec.key)
            self.cursor.execute(rec.sql_ins, rec)
        if (
                reciprocate and
                isinstance(rec, EdgeRecord) and
                self.cache['graphs'][rec.graph].__class__ in (
                    Graph,
                    MultiGraph
                )  # not directed
        ):
            self.set_record(
                rec._replace(
                    nodeB=rec.nodeA,
                    nodeA=rec.nodeB
                ),
                do_write=False,
                reciprocate=False
            )

# This file is part of gorm, an object relational mapper for graphs.
# Copyright (c) 2014 ZacharySpector@gmail.com
"""Wrapper to run SQL queries in a lightly abstracted way, such that
code that's more to do with the queries than with the data per se
doesn't pollute the other files so much.

"""
from collections import MutableMapping
from sqlite3 import IntegrityError as sqliteIntegError
from .reify import reify
try:
    # python 2
    import xjson
except ImportError:
    # python 3
    from gorm import xjson
import os
xjpath = os.path.dirname(xjson.__file__)
alchemyIntegError = None
try:
    from sqlalchemy.exc import IntegrityError as alchemyIntegError
except ImportError:
    pass


IntegrityError = (
    alchemyIntegError, sqliteIntegError
) if alchemyIntegError is not None else sqliteIntegError


class GlobalKeyValueStore(MutableMapping):
    """A dict-like object that keeps its contents in a table.

    Mostly this is for holding the current branch and revision.

    """
    def __init__(self, qe):
        """Remember the query engine."""
        self.qe = qe

    def __iter__(self):
        """Yield key field"""
        for (k, v) in self.qe.global_items():
            yield k

    def __len__(self):
        """Count rows"""
        return self.qe.ctglobal()

    def __getitem__(self, k):
        """Return value field corresponding to key field"""
        return self.qe.global_get(k)

    def __setitem__(self, k, v):
        """Insert or update record with key ``k`` so it has value ``v``"""
        self.qe.global_set(k, v)

    def __delitem__(self, k):
        """Delete record with key ``k``"""
        self.qe.global_del(k)


class QueryEngine(object):
    """Wrapper around either a DBAPI2.0 connection or an
    Alchemist. Provides functions to run queries using either.

    """
    json_path = xjpath

    def __init__(self, dbstring, connect_args, alchemy, json_dump=None, json_load=None):
        """If ``alchemy`` is True and ``dbstring`` is a legit database URI,
        instantiate an Alchemist and start a transaction with
        it. Otherwise use sqlite3.

        You may pass an already created sqlalchemy :class:`Engine`
        object in place of ``dbstring`` if you wish. I'll still create
        my own transaction though.

        """
        def alchem_init(dbstring, connect_args):
            from sqlalchemy import create_engine
            from sqlalchemy.engine.base import Engine
            from gorm.alchemy import Alchemist
            if isinstance(dbstring, Engine):
                self.engine = dbstring
            else:
                self.engine = create_engine(
                    dbstring,
                    connect_args=connect_args
                )
            self.alchemist = Alchemist(self.engine)
            self.transaction = self.alchemist.conn.begin()

        def lite_init(dbstring, connect_args):
            from sqlite3 import connect, Connection
            from json import loads
            self.strings = loads(
                open(self.json_path + '/sqlite.json', 'r').read()
            )
            if isinstance(dbstring, Connection):
                self.connection = dbstring
            else:
                if dbstring.startswith('sqlite:'):
                    slashidx = dbstring.rindex('/')
                    dbstring = dbstring[slashidx+1:]
                self.connection = connect(dbstring)

        if alchemy:
            try:
                alchem_init(dbstring, connect_args)
            except ImportError:
                lite_init(dbstring, connect_args)
        else:
            lite_init(dbstring, connect_args)

        self._branches = {}
        self.json_dump = json_dump if json_dump else xjson.json_dump
        self.json_load = json_load if json_load else xjson.json_load

    @reify
    def globl(self):
        return GlobalKeyValueStore(self)

    def sql(self, stringname, *args, **kwargs):
        """Wrapper for the various prewritten or compiled SQL calls.

        First argument is the name of the query, either a key in
        ``gorm.sql.sqlite_strings`` or a method name in
        ``gorm.alchemy.Alchemist``. The rest of the arguments are
        parameters to the query.

        """
        if hasattr(self, 'alchemist'):
            return getattr(self.alchemist, stringname)(*args, **kwargs)
        else:
            s = self.strings[stringname]
            return self.connection.cursor().execute(
                s.format(**kwargs) if kwargs else s, args
            )

    def timestream_data(self):
        for row in self.sql('allbranch'):
            yield tuple(row)

    def active_branches(self, branch, rev):
        """Yield a series of ``(branch, rev)`` pairs, starting with the
        ``branch`` and ``rev`` provided; proceeding to the parent
        branch and the revision therein when the provided branch
        began; and recursing through the entire genealogy of branches
        until we reach the branch 'master'.

        Though not private, this is a utility function that is
        unlikely to be useful unless you're adding functionality to
        gorm.

        """
        yield (branch, rev)
        while branch != 'master':
            if branch not in self._branches:
                (b, r) = self.parparrev(branch)
                self._branches[branch] = (b, self.json_load(r))
            (branch, rev) = self._branches[branch]
            yield (branch, rev)

    def have_graph(self, graph):
        """Return whether I have a graph by this name."""
        graph = self.json_dump(graph)
        return bool(self.sql('ctgraph', graph).fetchone()[0])

    def new_graph(self, graph, typ):
        """Declare a new graph by this name of this type."""
        graph = self.json_dump(graph)
        return self.sql('new_graph', graph, typ)

    def del_graph(self, graph):
        """Delete all records to do with the graph"""
        g = self.json_dump(graph)
        self.sql('del_edge_val_graph', g)
        self.sql('del_edge_graph', g)
        self.sql('del_node_val_graph', g)
        self.sql('del_edge_val_graph', g)
        self.sql('del_graph', g)

    def graph_type(self, graph):
        """What type of graph is this?"""
        graph = self.json_dump(graph)
        return self.sql('graph_type', graph).fetchone()[0]

    def have_branch(self, branch):
        """Return whether the branch thus named exists in the database."""
        return bool(self.sql('ctbranch', branch).fetchone()[0])

    def all_branches(self):
        """Return all the branch data in tuples of (branch, parent,
        parent_rev).

        """
        return self.sql('allbranch').fetchall()

    def global_get(self, key):
        """Return the value for the given key in the ``globals`` table."""
        key = self.json_dump(key)
        r = self.sql('global_get', key).fetchone()
        if r is None:
            raise KeyError("Not set")
        return self.json_load(r[0])

    def global_items(self):
        """Iterate over (key, value) pairs in the ``globals`` table."""
        for (k, v) in self.sql('global_items'):
            yield (self.json_load(k), self.json_load(v))

    def global_set(self, key, value):
        """Set ``key`` to ``value`` globally (not at any particular branch or
        revision)

        """
        (key, value) = map(self.json_dump, (key, value))
        try:
            return self.sql('global_ins', key, value)
        except IntegrityError:
            return self.sql('global_upd', value, key)

    def global_del(self, key):
        """Delete the global record for the key."""
        key = self.json_dump(key)
        return self.sql('global_del', key)

    def parrev(self, branch):
        """Return the parent of the branch."""
        return self.sql('parrev', branch).fetchone()[0]

    def parparrev(self, branch):
        """Return the parent and start revision of the branch."""
        return self.sql('parparrev', branch)

    def new_branch(self, branch, parent, parent_rev):
        """Declare that the ``branch`` is descended from ``parent`` at
        ``parent_rev``

        """
        return self.sql('new_branch', branch, parent, parent_rev)

    def graph_val_dump(self):
        """Yield the entire contents of the graph_val table."""
        for (graph, key, branch, rev, value) in self.sql('graph_val_dump'):
            yield (
                self.json_load(graph),
                self.json_load(key),
                branch,
                rev,
                self.json_load(value)
            )

    def graph_val_keys(self, graph, branch, rev):
        """Return an iterable of keys that are set on the graph at the given
        revision.

        """
        graph = self.json_dump(graph)
        seen = set()
        for (b, r) in self.active_branches(branch, rev):
            data = self.sql(
                'graph_val_items', graph, b, r
            )
            for (k, v) in data:
                if k not in seen:
                    yield self.json_load(k)
                seen.add(k)

    def graph_val_get(self, graph, key, branch, rev):
        """Return the value of a key that a graph has, as of the given
        revision.

        """
        (graph, key) = map(self.json_dump, (graph, key))
        for (b, r) in self.active_branches(branch, rev):
            for row in self.sql(
                'graph_val_get',
                graph,
                key,
                branch,
                rev
            ):
                if row is None:
                    raise KeyError("Key not set")
                return self.json_load(row[0])
        raise KeyError("Key never set")

    def graph_val_set(self, graph, key, branch, rev, value):
        """Set a key to a value on a graph at a particular revision."""
        (graph, key, value) = map(self.json_dump, (graph, key, value))
        try:
            self.sql('graph_val_ins', graph, key, branch, rev, value)
        except IntegrityError:
            self.sql('graph_val_upd', value, graph, key, branch, rev)

    def graph_val_del(self, graph, key, branch, rev):
        """Indicate that the key is unset."""
        (graph, key) = map(self.json_dump, (graph, key))
        try:
            self.sql('graph_val_ins', graph, key, branch, rev, None)
        except IntegrityError:
            self.sql('graph_val_upd', None, graph, key, branch, rev)

    def nodes_extant(self, graph, branch, rev):
        """Return an iterable of nodes that exist in this graph at this
        revision.

        """
        graph = self.json_dump(graph)
        seen = set()
        for (b, r) in self.active_branches(branch, rev):
            data = self.sql(
                'nodes_extant',
                graph,
                branch,
                rev
            )
            for (n,) in data:
                if n is not None and n not in seen:
                    yield self.json_load(n)
                seen.add(n)

    def node_exists(self, graph, node, branch, rev):
        """Return whether there's a node by this name in this graph at this
        revision.

        """
        (graph, node) = map(self.json_dump, (graph, node))
        for (b, r) in self.active_branches(branch, rev):
            for x in self.sql(
                'node_exists', graph, node, b, r
            ):
                return bool(x[0])
        return False

    def exist_node(self, graph, node, branch, rev, extant):
        """Declare that the node exists or doesn't.

        Inserts a new record or updates an old one, as needed.

        """
        (graph, node) = map(self.json_dump, (graph, node))
        try:
            self.sql('exist_node_ins', graph, node, branch, rev, extant)
        except IntegrityError:
            self.sql('exist_node_upd', extant, graph, node, branch, rev)

    def nodes_dump(self):
        """Dump the entire contents of the nodes table."""
        for (graph, node, branch, tick, extant) in self.sql('nodes_dump'):
            yield (
                self.json_load(graph),
                self.json_load(node),
                branch,
                tick,
                bool(extant)
            )

    def node_val_dump(self):
        """Yield the entire contents of the node_val table."""
        for (graph, node, key, branch, rev, value) in self.sql('node_val_dump'):
            yield (
                self.json_load(graph),
                self.json_load(node),
                self.json_load(key),
                branch,
                rev,
                self.json_load(value)
            )

    def node_val_keys(self, graph, node, branch, rev):
        """Return an iterable of keys that are set on the node at the given
        revision.

        """
        (graph, node) = map(self.json_dump, (graph, node))
        seen = set()
        for (b, r) in self.active_branches(branch, rev):
            for (k, v) in self.sql(
                    'node_val_items',
                    graph,
                    node,
                    branch,
                    rev
            ):
                if k not in seen and v is not None:
                    yield self.json_load(k)
                seen.add(k)

    def node_vals_ever(self, graph, node):
        """Iterate over all values set on a node through time."""
        (graph, node) = map(self.json_dump, (graph, node))
        for (key, branch, tick, value) in self.sql(
                'node_vals_ever', graph, node
        ):
            yield (self.json_load(key), branch, tick, self.json_load(value))

    def node_val_get(self, graph, node, key, branch, rev):
        """Get the value of the node's key as it was at the given revision."""
        (graph, node, key) = map(self.json_dump, (graph, node, key))
        for (b, r) in self.active_branches(branch, rev):
            for row in self.sql(
                'node_val_get',
                graph,
                node,
                key,
                branch,
                rev
            ):
                if row[0] is None:
                    raise KeyError("Key not set")
                return self.json_load(row[0])
        raise KeyError("Key {} never set".format(key))

    def node_val_set(self, graph, node, key, branch, rev, value):
        """Set the value of a key on a node at a particular revision."""
        (graph, node, key, value) = map(
            self.json_dump,
            (graph, node, key, value)
        )
        try:
            return self.sql(
                'node_val_ins', graph, node, key, branch, rev, value
            )
        except IntegrityError:
            return self.sql(
                'node_val_upd', value, graph, node, key, branch, rev
            )

    def node_val_del(self, graph, node, key, branch, rev):
        """Indicate that the key has no value for the node at the revision."""
        (graph, node, key) = map(self.json_dump, (graph, node, key))
        try:
            return self.sql(
                'node_val_ins', graph, node, key, branch, rev, None
            )
        except IntegrityError:
            return self.sql(
                'node_val_upd', None, graph, node, key, branch, rev
            )

    def edges_dump(self):
        """Dump the entire contents of the edges table."""
        for (graph, nodeA, nodeB, idx, branch, rev, extant) in self.sql('edges_dump'):
            yield (
                self.json_load(graph),
                self.json_load(nodeA),
                self.json_load(nodeB),
                idx,
                branch,
                rev,
                bool(extant)
            )

    def edges_extant(self, graph, branch, rev):
        """Return an iterable of nodes that have edges from them, in this
        graph, at this revision.

        """
        graph = self.json_dump(graph)
        seen = set()
        for (b, r) in self.active_branches(branch, rev):
            for row in self.sql(
                'edges_extant', graph, branch, rev
            ):
                if row[0] not in seen and row[1]:
                    yield self.json_load(row[0])
                seen.add(row[0])

    def edge_exists(self, graph, nodeA, nodeB, idx, branch, rev):
        """Return whether the edge exists now, or None if there's no data
        about it in this branch.

        """
        (graph, nodeA, nodeB) = map(self.json_dump, (graph, nodeA, nodeB))
        for (b, r) in self.active_branches(branch, rev):
            for row in self.sql(
                'edge_exists',
                graph,
                nodeA,
                nodeB,
                idx,
                b,
                r
            ):
                return bool(row[1])
        return False

    def nodeAs(self, graph, nodeB, branch, rev):
        """Return an iterable of nodes that have an edge leading to the given
        node.

        """
        (graph, nodeB) = map(self.json_dump, (graph, nodeB))
        seen = set()
        for (b, r) in self.active_branches(branch, rev):
            for row in self.sql(
                'nodeAs',
                graph,
                nodeB,
                b,
                r
            ):
                if row[0] not in seen and row[1]:
                    yield self.json_load(row[0])
                seen.add(row[0])

    def nodeBs(self, graph, nodeA, branch, rev):
        """Return an iterable of nodes you can get to from the given one."""
        (graph, nodeA) = map(self.json_dump, (graph, nodeA))
        seen = set()
        for (b, r) in self.active_branches(branch, rev):
            for row in self.sql(
                'nodeBs', graph, nodeA, b, r
            ):
                if row[0] not in seen and row[1]:
                    yield self.json_load(row[0])
                seen.add(row[0])

    def multi_edges(self, graph, nodeA, nodeB, branch, rev):
        """Return an iterable of edge indices for all edges between these two
        nodes.

        """
        (graph, nodeA, nodeB) = map(self.json_dump, (graph, nodeA, nodeB))
        seen = set()
        for (b, r) in self.active_branches(branch, rev):
            for row in self.sql(
                'multi_edges', graph, nodeA, nodeB, branch, rev
            ):
                if row[0] not in seen and row[1]:
                    yield row[0]
                seen.add(row[0])

    def exist_edge(self, graph, nodeA, nodeB, idx, branch, rev, extant):
        """Declare whether or not this edge exists."""
        (graph, nodeA, nodeB) = map(self.json_dump, (graph, nodeA, nodeB))
        try:
            self.sql(
                'edge_exist_ins', graph, nodeA, nodeB, idx, branch, rev, extant
            )
        except IntegrityError:
            self.sql(
                'edge_exist_upd', extant, graph, nodeA, nodeB, idx, branch, rev
            )

    def edge_val_dump(self):
        """Yield the entire contents of the edge_val table."""
        for (graph, nodeA, nodeB, idx, key, branch, rev, value) in self.sql('edge_val_dump'):
            yield (
                self.json_load(graph),
                self.json_load(nodeA),
                self.json_load(nodeB),
                idx,
                self.json_load(key),
                branch,
                rev,
                self.json_load(value)
            )

    def edge_val_keys(self, graph, nodeA, nodeB, idx, branch, rev):
        """Return an iterable of keys this edge has."""
        (graph, nodeA, nodeB) = map(self.json_dump, (graph, nodeA, nodeB))
        seen = set()
        for (b, r) in self.active_branches(branch, rev):
            for row in self.sql(
                'edge_val_items', graph, nodeA, nodeB, idx, b, r
            ):
                if row[0] not in seen:
                    yield self.json_load(row[0])
                seen.add(row[0])

    def edge_val_get(self, graph, nodeA, nodeB, idx, key, branch, rev):
        """Return the value of this key of this edge."""
        (graph, nodeA, nodeB, key) = map(self.json_dump, (graph, nodeA, nodeB, key))
        for (b, r) in self.active_branches(branch, rev):
            for row in self.sql(
                'edge_val_get', graph, nodeA, nodeB, idx, key, b, r
            ):
                if row[0] is None:
                    raise KeyError("Key not set")
                return self.json_load(row[0])
        raise KeyError("Key never set")

    def edge_val_set(self, graph, nodeA, nodeB, idx, key, branch, rev, value):
        """Set this key of this edge to this value."""
        (graph, nodeA, nodeB, key, value) = map(
            self.json_dump,
            (graph, nodeA, nodeB, key, value)
        )
        try:
            self.sql(
                'edge_val_ins',
                graph,
                nodeA,
                nodeB,
                idx,
                key,
                branch,
                rev,
                value
            )
        except IntegrityError:
            self.sql(
                'edge_val_upd',
                value,
                graph,
                nodeA,
                nodeB,
                idx,
                key,
                branch,
                rev
            )

    def edge_val_del(self, graph, nodeA, nodeB, idx, key, branch, rev):
        """Declare that the key no longer applies to this edge, as of this
        branch and revision.

        """
        (graph, nodeA, nodeB, key) = map(
            self.json_dump,
            (graph, nodeA, nodeB, key)
        )
        try:
            self.sql(
                'edge_val_ins',
                graph,
                nodeA,
                nodeB,
                idx,
                key,
                branch,
                rev,
                None
            )
        except IntegrityError:
            self.sql(
                'edge_val_upd',
                None,
                graph,
                nodeA,
                nodeB,
                idx,
                key,
                branch,
                rev
            )

    def initdb(self):
        """Create tables and indices."""
        if hasattr(self, 'alchemist'):
            self.alchemist.meta.create_all(self.engine)
            if 'branch' not in self.globl:
                self.globl['branch'] = 'master'
            if 'rev' not in self.globl:
                self.globl['rev'] = 0
            return
        from sqlite3 import OperationalError
        cursor = self.connection.cursor()
        try:
            cursor.execute('SELECT * FROM global;')
        except OperationalError:
            cursor.execute(self.strings['create_global'])
        if 'branch' not in self.globl:
            self.globl['branch'] = 'master'
        if 'rev' not in self.globl:
            self.globl['rev'] = 0
        try:
            cursor.execute('SELECT * FROM branches;')
        except OperationalError:
            cursor.execute(self.strings['create_branches'])
            cursor.execute(
                "INSERT INTO branches (branch, parent, parent_rev) "
                "VALUES ('master', 'master', 0)"
            )
        try:
            cursor.execute('SELECT * FROM graphs;')
        except OperationalError:
            cursor.execute(self.strings['create_graphs'])
        try:
            cursor.execute('SELECT * FROM graph_val;')
        except OperationalError:
            cursor.execute(self.strings['create_graph_val'])
            cursor.execute(self.strings['index_graph_val'])
        try:
            cursor.execute('SELECT * FROM nodes;')
        except OperationalError:
            cursor.execute(self.strings['create_nodes'])
            cursor.execute(self.strings['index_nodes'])

        try:
            cursor.execute('SELECT * FROM node_val;')
        except OperationalError:
            cursor.execute(self.strings['create_node_val'])
            cursor.execute(self.strings['index_node_val'])
        try:
            cursor.execute('SELECT * FROM edges;')
        except OperationalError:
            cursor.execute(self.strings['create_edges'])
            cursor.execute(self.strings['index_edges'])
        try:
            cursor.execute('SELECT * FROM edge_val;')
        except OperationalError:
            cursor.execute(self.strings['create_edge_val'])
            cursor.execute(self.strings['index_edge_val'])

    def commit(self):
        """Commit the transaction"""
        if hasattr(self, 'transaction'):
            self.transaction.commit()
        else:
            self.connection.commit()

    def close(self):
        """Commit the transaction, then close the connection"""
        self.commit()
        if hasattr(self, 'connection'):
            self.connection.close()

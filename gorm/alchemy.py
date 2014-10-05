# This file is part of gorm, an object relational mapper for versioned graphs.
# Copyright (C) 2014 Zachary Spector.
from sqlalchemy import (
    Table,
    Index,
    Column,
    CheckConstraint,
    ForeignKeyConstraint,
    Integer,
    Boolean,
    String,
    MetaData,
    ForeignKey,
    select,
    func,
    and_,
    null
)
from sqlalchemy.sql import bindparam

length = 50

meta = MetaData()

table_global = Table(
    'global', meta,
    Column('key', String(length), primary_key=True),
    Column('value', String(length), nullable=True)
)

table_branches = Table(
    'branches', meta,
    Column('branch', String(length), ForeignKey('branches.parent'),
           primary_key=True, default='master'
           ),
    Column('parent', String(length), default='master'),
    Column('parent_rev', Integer, default=0)
)

table_graphs = Table(
    'graphs', meta,
    Column('graph', String(length), primary_key=True),
    Column('type', String(length), default='Graph'),
    CheckConstraint(
        "type IN ('Graph', 'DiGraph', 'MultiGraph', 'MultiDiGraph')"
    )
)

table_graph_val = Table(
    'graph_val', meta,
    Column('graph', String(length), ForeignKey('graphs.graph'),
           primary_key=True),
    Column('key', String(length), primary_key=True),
    Column('branch', String(length), ForeignKey('branches.branch'),
           primary_key=True, default='master'),
    Column('rev', Integer, primary_key=True, default=0),
    Column('value', String(length), nullable=True)
)

index_graph_val = Index(
    "graph_val_idx",
    table_graph_val.c.graph,
    table_graph_val.c.key
)

table_nodes = Table(
    'nodes', meta,
    Column('graph', String(length), ForeignKey('graphs.graph'),
           primary_key=True),
    Column('node', String(length), primary_key=True),
    Column('branch', String(length), ForeignKey('branches.branch'),
           primary_key=True, default='master'),
    Column('rev', Integer, primary_key=True, default=0),
    Column('extant', Boolean)
)

index_nodes = Index(
    "nodes_idx",
    table_nodes.c.graph,
    table_nodes.c.node
)

table_node_val = Table(
    'node_val', meta,
    Column('graph', String(length), primary_key=True),
    Column('node', String(length), primary_key=True),
    Column('key', String(length), primary_key=True),
    Column('branch', String(length), ForeignKey('branches.branch'),
           primary_key=True, default='master'),
    Column('rev', Integer, primary_key=True, default=0),
    Column('value', String(length), nullable=True),
    ForeignKeyConstraint(['graph', 'node'], ['nodes.graph', 'nodes.node'])
)

index_node_val = Index(
    "node_val_idx",
    table_node_val.c.graph,
    table_node_val.c.node
)

table_edges = Table(
    'edges', meta,
    Column('graph', String(length), ForeignKey('graphs.graph'),
           primary_key=True),
    Column('nodeA', String(length), primary_key=True),
    Column('nodeB', String(length), primary_key=True),
    Column('idx', Integer, primary_key=True),
    Column('branch', String(length), ForeignKey('branches.branch'),
           primary_key=True, default='master'),
    Column('rev', Integer, primary_key=True, default=0),
    Column('extant', Boolean),
    ForeignKeyConstraint(['graph', 'nodeA'], ['nodes.graph', 'nodes.node']),
    ForeignKeyConstraint(['graph', 'nodeB'], ['nodes.graph', 'nodes.node'])
)

index_edges = Index(
    "edges_idx",
    table_edges.c.graph,
    table_edges.c.nodeA,
    table_edges.c.nodeB,
    table_edges.c.idx
)

table_edge_val = Table(
    'edge_val', meta,
    Column('graph', String(length), primary_key=True),
    Column('nodeA', String(length), primary_key=True),
    Column('nodeB', String(length), primary_key=True),
    Column('idx', Integer, primary_key=True),
    Column('key', String(length), primary_key=True),
    Column('branch', String(length), ForeignKey('branches.branch'),
           primary_key=True, default='master'),
    Column('rev', Integer, primary_key=True, default=0),
    Column('value', String(length), nullable=True),
    ForeignKeyConstraint(
        ['graph', 'nodeA', 'nodeB', 'idx'],
        ['edges.graph', 'edges.nodeA', 'edges.nodeB', 'edges.idx']
    )
)

index_edge_val = Index(
    "edge_val_idx",
    table_edge_val.c.graph,
    table_edge_val.c.nodeA,
    table_edge_val.c.nodeB,
    table_edge_val.c.idx,
    table_edge_val.c.key
)


class Alchemist(object):
    """Holds an engine and runs queries on it.

    """
    def __init__(self, engine):
        """Open a connection.

        Store a pointer to the metadata object object locally, for
        convenience.

        """
        self.engine = engine
        self.conn = self.engine.connect()
        self.meta = meta

    def ctbranch(self, branch):
        """Query to count the number of branches that exist."""
        if not hasattr(self, '_ctbranch_compiled'):
            self._ctbranch_compiled = select(
                [func.COUNT(table_branches.c.branch)]
            ).where(
                table_branches.c.branch == bindparam('branch')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._ctbranch_compiled,
            branch=branch
        )

    def ctgraph(self, graph):
        """Query to count the number of graphs that have been created."""
        if not hasattr(self, '_ctgraph_compiled'):
            self._ctgraph_compiled = select(
                [func.COUNT(table_graphs.c.graph)]
            ).where(
                table_graphs.c.graph == bindparam('graph')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._ctgraph_compiled,
            graph=graph
        )

    def allbranch(self):
        """Iterate over all available branch data."""
        if not hasattr(self, '_allbranch_compiled'):
            self._allbranch_compiled = select(
                [
                    table_branches.c.branch,
                    table_branches.c.parent,
                    table_branches.c.parent_rev
                ]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._allbranch_compiled
        )

    def global_get(self, key):
        """Get the value for a global key."""
        if not hasattr(self, '_global_get_compiled'):
            self._global_get_compiled = select(
                [table_global.c.value]
            ).where(
                table_global.c.key == bindparam('key')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._global_get_compiled,
            key=key
        )

    def global_items(self):
        """Iterate over key-value pairs set globally."""
        if not hasattr(self, '_global_items_compiled'):
            self._global_items_compiled = select(
                [
                    table_global.c.key,
                    table_global.c.value
                ]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._global_items_compiled
        )

    def ctglobal(self):
        """Count keys set globally."""
        if not hasattr(self, '_ctglobal_compiled'):
            self._ctglobal_compiled = select(
                [func.COUNT(table_global.c.key)]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._ctglobal_compiled
        )

    def new_graph(self, graph, typ):
        """Create a graph of a given type."""
        if not hasattr(self, '_new_graph_compiled'):
            self._new_graph_compiled = table_graphs.insert().values(
                graph=bindparam('graph'),
                type=bindparam('type')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._new_graph_compiled,
            graph=graph,
            type=typ
        )

    def graph_type(self, graph):
        """Fetch the type of the named graph."""
        if not hasattr(self, '_graph_type_compiled'):
            self._graph_type_compiled = select(
                [table_graphs.c.type]
            ).where(
                table_graphs.c.graph == bindparam('g')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._graph_type_compiled,
            g=graph
        )

    def new_branch(self, branch, parent, parent_rev):
        """Declare that the branch ``branch`` is a child of ``parent``
        starting at revision ``parent_rev``.

        """
        if not hasattr(self, '_new_branch_compiled'):
            self._new_branch_compiled = table_branches.insert().values(
                branch=bindparam('branch'),
                parent=bindparam('parent'),
                parent_rev=bindparam('parent_rev')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._new_branch_compiled,
            branch=branch,
            parent=parent,
            parent_rev=parent_rev
        )

    def del_edge_val_graph(self, graph):
        """Delete all edge attributes from ``graph``."""
        if not hasattr(self, '_del_edge_val_graph_compiled'):
            self._del_edge_val_graph_compiled = (
                table_edge_val.delete().where(
                    table_edge_val.c.graph == bindparam('graph')
                ).compile(dialect=self.engine.dialect)
            )
        return self.conn.execute(
            self._del_edge_val_graph_compiled,
            graph=graph
        )

    def del_node_val_graph(self, graph):
        """Delete all node attributes from ``graph``."""
        if not hasattr(self, '_del_node_val_graph_compiled'):
            self._del_node_val_graph_compiled = table_node_val.delete().where(
                table_node_val.c.graph == bindparam('graph')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._del_node_val_graph_compiled,
            graph=graph
        )

    def del_node_graph(self, graph):
        """Delete all nodes from ``graph``."""
        if not hasattr(self, '_del_node_graph_compiled'):
            self._del_node_graph_compiled = table_nodes.delete().where(
                table_nodes.c.graph == bindparam('graph')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._del_node_graph_compiled,
            graph=graph
        )

    def del_graph(self, graph):
        """Delete the graph header."""
        if not hasattr(self, '_del_graph_compiled'):
            self._del_graph_compiled = table_graphs.delete().where(
                table_graphs.c.graph == bindparam('graph')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._del_graph_compiled,
            graph=graph
        )

    def parrev(self, branch):
        """Fetch the revision at which ``branch`` forks off from its
        parent.

        """
        if not hasattr(self, '_parrev_compiled'):
            self._parrev_compiled = select(
                [table_branches.c.parent_rev]
            ).where(
                table_branches.c.branch == bindparam('branch')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._parrev_compiled,
            branch=branch
        )

    def parparrev(self, branch):
        """Fetch the name of ``branch``'s parent, and the revision at which
        they part.

        """
        if not hasattr(self, '_parparrev_compiled'):
            self._parparrev_compiled = select(
                [table_branches.c.parent, table_branches.c.parent_rev]
            ).where(
                table_branches.c.branch == bindparam('branch')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._parparrev_compiled,
            branch=branch
        )

    def global_ins(self, key, value):
        """Insert a record into the globals table indicating that
        ``key=value``.

        """
        if not hasattr(self, '_global_ins_compiled'):
            self._global_ins_compiled = table_global.insert().values(
                key=bindparam('k'),
                value=bindparam('v')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._global_ins_compiled,
            k=key,
            v=value
        )

    def global_upd(self, key, value):
        """Update the existing global record for ``key`` so that it is set to
        ``value``.

        """
        if not hasattr(self, '_global_upd_compiled'):
            self._global_upd_compiled = table_global.update().values(
                value=bindparam('v')
            ).where(
                table_global.c.key == bindparam('k')
            )
        return self.conn.execute(
            self._global_upd_compiled,
            k=key,
            v=value
        )

    def global_del(self, key):
        """Delete the record for global variable ``key``."""
        if not hasattr(self, '_global_del_compiled'):
            self._global_del_compiled = table_global.delete().where(
                key=bindparam('k')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._global_del_compiled,
            k=key
        )

    def _recent_nodes(self, node=False):
        """Private method. Returns a query to get the most recent info on
        what nodes exist in some graph at some revision.

        """
        hirev_where = [
            table_nodes.c.graph == bindparam('g'),
            table_nodes.c.branch == bindparam('b'),
            table_nodes.c.rev <= bindparam('r')
        ]
        if node:
            hirev_where.append(table_nodes.c.node == bindparam('n'))
        nodes_hirev = select(
            [
                table_nodes.c.graph,
                table_nodes.c.node,
                table_nodes.c.branch,
                func.MAX(table_nodes.c.rev).label('rev')
            ]
        ).where(
            and_(
                *hirev_where
            )
        ).group_by(
            table_nodes.c.graph,
            table_nodes.c.node,
            table_nodes.c.branch
        ).alias('hirev')
        return select(
            [
                table_nodes.c.graph,
                table_nodes.c.node,
                table_nodes.c.branch,
                table_nodes.c.rev,
                table_nodes.c.extant
            ]
        ).select_from(
            table_nodes.join(
                nodes_hirev,
                and_(
                    table_nodes.c.graph == nodes_hirev.c.graph,
                    table_nodes.c.node == nodes_hirev.c.node,
                    table_nodes.c.branch == nodes_hirev.c.branch,
                    table_nodes.c.rev == nodes_hirev.c.rev
                )
            )
        )

    def nodes_extant(self, graph, branch, rev):
        """Query for nodes that exist in ``graph`` at ``(branch, rev)``."""
        if not hasattr(self, '_nodes_extant_compiled'):
            rn = self._recent_nodes()
            self._nodes_extant_compiled = select(
                [rn.c.node]
            ).where(
                rn.c.extant
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._nodes_extant_compiled,
            g=graph,
            b=branch,
            r=rev
        )

    def node_exists(self, graph, node, branch, rev):
        """Query for whether or not ``node`` exists in ``graph`` at ``(branch,
        rev)``.

        """
        if not hasattr(self, '_node_exists_compiled'):
            rn = self._recent_nodes(node=True)
            self._node_exists_compiled = select(
                [rn.c.extant]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._node_exists_compiled,
            g=graph,
            n=node,
            b=branch,
            r=rev
        )

    def exist_node_ins(self, graph, node, branch, rev, extant):
        """Insert a record to indicate whether or not ``node`` exists in
        ``graph`` at ``(branch, rev)``.

        """
        if not hasattr(self, '_exist_node_ins_compiled'):
            self._exist_node_ins_compiled = (
                table_nodes.insert().values(
                    graph=bindparam('g'),
                    node=bindparam('n'),
                    branch=bindparam('b'),
                    rev=bindparam('r'),
                    extant=bindparam('x')
                ).compile(dialect=self.engine.dialect)
            )
        return self.conn.execute(
            self._exist_node_ins_compiled,
            g=graph,
            n=node,
            b=branch,
            r=rev,
            x=extant
        )

    def exist_node_upd(self, extant, graph, node, branch, rev):
        """Update the record previously inserted by ``exist_node_ins``,
        indicating whether ``node`` exists in ``graph`` at ``(branch,
        rev)``.

        """
        if not hasattr(self, '_exist_node_upd_compiled'):
            self._exist_node_upd_compiled = table_nodes.update().values(
                extant=bindparam('x')
            ).where(
                and_(
                    table_nodes.c.graph == bindparam('g'),
                    table_nodes.c.node == bindparam('n'),
                    table_nodes.c.branch == bindparam('b'),
                    table_nodes.c.rev == bindparam('r')
                )
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._exist_node_upd_compiled,
            x=extant,
            g=graph,
            n=node,
            b=branch,
            r=rev
        )

    def _recent_graph_val(self, key=False):
        """Private method. Return a query for the most recent graph_val
        records in some graph.

        """
        hirev_where = [
            table_graph_val.c.graph == bindparam('g'),
            table_graph_val.c.branch == bindparam('b'),
            table_graph_val.c.rev <= bindparam('r')
        ]
        if key:
            hirev_where.append(table_graph_val.c.key == bindparam('k'))
        hirev_graph_val = select(
            [
                table_graph_val.c.graph,
                table_graph_val.c.key,
                table_graph_val.c.branch,
                func.MAX(table_graph_val.c.rev).label('rev')
            ]
        ).where(
            and_(*hirev_where)
        ).group_by(
            table_graph_val.c.graph,
            table_graph_val.c.key,
            table_graph_val.c.branch
        ).alias()
        return select(
            [
                table_graph_val.c.graph,
                table_graph_val.c.key,
                table_graph_val.c.branch,
                table_graph_val.c.rev,
                table_graph_val.c.value
            ]
        ).select_from(
            table_graph_val.join(
                hirev_graph_val,
                and_(
                    table_graph_val.c.graph == hirev_graph_val.c.graph,
                    table_graph_val.c.key == hirev_graph_val.c.key,
                    table_graph_val.c.branch == hirev_graph_val.c.branch,
                    table_graph_val.c.rev == hirev_graph_val.c.rev
                )
            )
        )

    def graph_val_items(self, graph, branch, rev):
        """Query the most recent keys and values for the attributes of
        ``graph`` at ``(branch, rev)``.

        """
        if not hasattr(self, '_graph_val_items_compiled'):
            rgv = self._recent_graph_val()
            self._graph_val_items_compiled = select(
                [rgv.c.key, rgv.c.value]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._graph_val_items_compiled,
            g=graph,
            b=branch,
            r=rev
        )

    def graph_val_get(self, graph, key, branch, rev):
        """Query the most recent value for ``graph``'s ``key`` as of
        ``(branch, rev)``

        """
        if not hasattr(self, '_graph_val_get_compiled'):
            rgv = self._recent_graph_val(key=True)
            self._graph_val_get_compiled = select(
                [rgv.c.value]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._graph_val_get_compiled,
            g=graph,
            k=key,
            b=branch,
            r=rev
        )

    def graph_val_ins(self, graph, key, branch, rev, value):
        """Insert a record to indicate that ``key=value`` on ``graph`` as of
        ``(branch, rev)``

        """
        if not hasattr(self, '_graph_val_ins_compiled'):
            self._graph_val_ins_compiled = (
                table_graph_val.insert().values(
                    graph=bindparam('g'),
                    key=bindparam('k'),
                    branch=bindparam('b'),
                    rev=bindparam('r'),
                    value=bindparam('v')
                ).compile(dialect=self.engine.dialect)
            )
        return self.conn.execute(
            self._graph_val_ins_compiled,
            g=graph,
            k=key,
            b=branch,
            r=rev,
            v=value
        )

    def graph_val_upd(self, value, graph, key, branch, rev):
        """Update the record previously inserted by ``graph_val_ins``"""
        if not hasattr(self, '_graph_val_upd_compiled'):
            self._graph_val_upd_compiled = table_graph_val.update().values(
                value=bindparam('v')
            ).where(
                and_(
                    table_graph_val.c.graph == bindparam('g'),
                    table_graph_val.c.key == bindparam('k'),
                    table_graph_val.c.branch == bindparam('b'),
                    table_graph_val.c.rev == bindparam('r')
                )
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._graph_val_upd_compiled,
            v=value,
            g=graph,
            k=key,
            b=branch,
            r=rev
        )

    def _recent_node_val(self, key=False):
        """Private method. Return a query for getting the most recent value of
        keys on a node.

        """
        hirev_where = [
            table_node_val.c.graph == bindparam('g'),
            table_node_val.c.node == bindparam('n'),
            table_node_val.c.branch == bindparam('b'),
            table_node_val.c.rev <= bindparam('r')
        ]
        if key:
            hirev_where.append(table_node_val.c.key == bindparam('k'))
        node_val_hirev = select(
            [
                table_node_val.c.graph,
                table_node_val.c.node,
                table_node_val.c.branch,
                table_node_val.c.key,
                func.MAX(table_node_val.c.rev).label('rev')
            ]
        ).where(
            and_(*hirev_where)
        ).group_by(
            table_node_val.c.graph,
            table_node_val.c.node,
            table_node_val.c.branch,
            table_node_val.c.key
        ).alias('hirev')
        return select(
            [
                table_node_val.c.graph,
                table_node_val.c.node,
                table_node_val.c.key,
                table_node_val.c.branch,
                table_node_val.c.rev,
                table_node_val.c.value
            ]
        ).select_from(
            table_node_val.join(
                node_val_hirev,
                and_(
                    table_node_val.c.graph == node_val_hirev.c.graph,
                    table_node_val.c.node == node_val_hirev.c.node,
                    table_node_val.c.key == node_val_hirev.c.key,
                    table_node_val.c.branch == node_val_hirev.c.branch,
                    table_node_val.c.rev == node_val_hirev.c.rev
                )
            )
        )

    def node_val_items(self, graph, node, branch, rev):
        """Get all the most recent values of all the keys on ``node`` in
        ``graph`` as of ``(branch, rev)``

        """
        if not hasattr(self, '_node_val_items_compiled'):
            rnv = self._recent_node_val()
            self._node_val_items_compiled = select(
                [rnv.c.key, rnv.c.value]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._node_val_items_compiled,
            g=graph,
            n=node,
            b=branch,
            r=rev
        )

    def node_val_get(self, graph, node, key, branch, rev):
        """Get the most recent value for ``key`` on ``node`` in ``graph`` as
        of ``(branch, rev)``

        """
        if not hasattr(self, '_node_val_get_compiled'):
            rnv = self._recent_node_val(key=True)
            self._node_val_get_compiled = select(
                [rnv.c.value]
            ).where(
                rnv.c.value != null()
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._node_val_get_compiled,
            g=graph,
            n=node,
            k=key,
            b=branch,
            r=rev
        )

    def node_val_ins(self, graph, node, key, branch, rev, value):
        """Insert a record to indicate that the value of ``key`` on ``node``
        in ``graph`` as of ``(branch, rev)`` is ``value``.

        """
        if not hasattr(self, '_node_val_ins_compiled'):
            self._node_val_ins_compiled = table_node_val.insert().values(
                graph=bindparam('g'),
                node=bindparam('n'),
                key=bindparam('k'),
                branch=bindparam('b'),
                rev=bindparam('r'),
                value=bindparam('v')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._node_val_ins_compiled,
            g=graph,
            n=node,
            k=key,
            b=branch,
            r=rev,
            v=value
        )

    def node_val_upd(self, value, graph, node, key, branch, rev):
        """Update the record previously inserted by ``node_val_ins``"""
        if not hasattr(self, '_node_val_upd_compiled'):
            self._node_val_upd_compiled = table_node_val.update().values(
                value=bindparam('v')
            ).where(
                and_(
                    graph=bindparam('g'),
                    node=bindparam('n'),
                    key=bindparam('k'),
                    branch=bindparam('b'),
                    rev=bindparam('r')
                )
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._node_val_upd_compiled,
            v=value,
            g=graph,
            n=node,
            k=key,
            b=branch,
            r=rev
        )

    def _edges_recent(self, orig=False, dest=False, idx=False):
        """Private method. Return a query to get the most recent edge
        existence data for some graph.

        """
        hirev_where = [
            table_edges.c.graph == bindparam('g'),
            table_edges.c.branch == bindparam('b'),
            table_edges.c.rev <= bindparam('r')
        ]
        if orig:
            hirev_where.append(table_edges.c.nodeA == bindparam('orig'))
        if dest:
            hirev_where.append(table_edges.c.nodeB == bindparam('dest'))
        if idx:
            hirev_where.append(table_edges.c.idx == bindparam('i'))
        edges_hirev = select(
            [
                table_edges.c.graph,
                table_edges.c.nodeA,
                table_edges.c.nodeB,
                table_edges.c.idx,
                table_edges.c.branch,
                func.MAX(table_edges.c.rev).label('rev')
            ]
        ).where(
            and_(*hirev_where)
        ).group_by(
            table_edges.c.graph,
            table_edges.c.nodeA,
            table_edges.c.nodeB,
            table_edges.c.idx,
            table_edges.c.branch
        ).alias('hirev')
        return select(
            [
                table_edges.c.graph,
                table_edges.c.nodeA,
                table_edges.c.nodeB,
                table_edges.c.idx,
                table_edges.c.branch,
                table_edges.c.rev,
                table_edges.c.extant
            ]
        ).select_from(
            table_edges.join(
                edges_hirev,
                and_(
                    table_edges.c.graph == edges_hirev.c.graph,
                    table_edges.c.nodeA == edges_hirev.c.nodeA,
                    table_edges.c.nodeB == edges_hirev.c.nodeB,
                    table_edges.c.idx == edges_hirev.c.idx,
                    table_edges.c.branch == edges_hirev.c.branch,
                    table_edges.c.rev == edges_hirev.c.rev
                )
            )
        )

    def edge_exists(self, graph, nodeA, nodeB, idx, branch, rev):
        """Query for whether a particular edge exists at a particular
        ``(branch, rev)``

        """
        if not hasattr(self, '_edge_exists_compiled'):
            self._edge_extant_compiled = select(
                [self._edges_recent(orig=True, dest=True, idx=True).c.extant]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._edge_exists_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            b=branch,
            r=rev
        )

    def edges_extant(self, graph, branch, rev):
        """Query for all edges that exist in ``graph`` as of ``(branch,
        rev)``

        """
        if not hasattr(self, '_edges_extant_compiled'):
            er = self._edges_recent()
            self._edges_extant_compiled = select(
                [er.c.nodeA, er.c.extant]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._edges_extant_compiled,
            g=graph,
            b=branch,
            r=rev
        )

    def nodeAs(self, graph, nodeB, branch, rev):
        """Query for edges that end at ``nodeB`` in ``graph`` as of ``(branch,
        rev)``

        """
        if not hasattr(self, '_nodeAs_compiled'):
            er = self._edges_recent(dest=True)
            self._nodeAs_compiled = select(
                [er.c.nodeA, er.c.extant]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._nodeAs_compiled,
            g=graph,
            dest=nodeB,
            b=branch,
            r=rev
        )

    def nodeBs(self, graph, nodeA, branch, rev):
        """Query for the nodes at which edges that originate from ``nodeA``
        end.

        """
        if not hasattr(self, '_nodeBs_compiled'):
            er = self._edges_recent(orig=True)
            self._nodeBs_compiled = select(
                [er.c.nodeB, er.c.extant]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._nodeBs_compiled,
            g=graph,
            orig=nodeA,
            b=branch,
            r=rev
        )

    def multi_edges(self, graph, nodeA, nodeB, branch, rev):
        """Query for all edges from ``nodeA`` to ``nodeB``. Only makes sense
        if we're dealing with a :class:`MultiGraph` or
        :class:`MultiDiGraph`.

        """
        if not hasattr(self, '_multi_edges_compiled'):
            er = self._edges_recent(orig=True, dest=True)
            self._multi_edges_compiled = select(
                [er.c.idx, er.c.extant]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._multi_edges_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            b=branch,
            r=rev
        )

    def edge_exist_ins(self, graph, nodeA, nodeB, idx, branch, rev, extant):
        """Indicate that there is (or isn't) an edge from ``nodeA`` to
        ``nodeB`` in ``graph`` as of ``(branch, rev)``.

        ``idx`` should be ``0`` unless ``graph`` is a
        :class:`MultiGraph` or :class:`MultiDiGraph`.

        """
        if not hasattr(self, '_edge_exist_ins_compiled'):
            self._edge_exist_ins_compiled = table_edges.insert().values(
                graph=bindparam('g'),
                nodeA=bindparam('orig'),
                nodeB=bindparam('dest'),
                idx=bindparam('i'),
                branch=bindparam('b'),
                rev=bindparam('r'),
                extant=bindparam('x')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._edge_exist_ins_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            b=branch,
            r=rev,
            x=extant
        )

    def edge_exist_upd(self, extant, graph, nodeA, nodeB, idx, branch, rev):
        """Update a record previously inserted with ``edge_exist_ins``."""
        if not hasattr(self, '_edge_exist_upd_compiled'):
            self._edge_exist_upd_compiled = table_edges.update().values(
                extant=bindparam('x')
            ).where(
                and_(
                    table_edges.c.graph == bindparam('g'),
                    table_edges.c.nodeA == bindparam('orig'),
                    table_edges.c.nodeB == bindparam('dest'),
                    table_edges.c.idx == bindparam('i'),
                    table_edges.c.branch == bindparam('b'),
                    table_edges.c.rev == bindparam('r')
                )
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._edge_exist_upd_compiled,
            x=extant,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            b=branch,
            r=rev
        )

    def _edge_val_recent(self, key=False):
        """Private method. Make a query for getting the most recent record
        relevant to edge values of some edge.

        """
        hirev_where = [
            table_edge_val.c.graph == bindparam('g'),
            table_edge_val.c.nodeA == bindparam('orig'),
            table_edge_val.c.nodeB == bindparam('dest'),
            table_edge_val.c.idx == bindparam('i'),
            table_edge_val.c.branch == bindparam('b'),
            table_edge_val.c.rev <= bindparam('r')
        ]
        if key:
            hirev_where.append(table_edge_val.c.key == bindparam('k'))
        hirev_edge_val = select(
            [
                table_edge_val.c.graph,
                table_edge_val.c.nodeA,
                table_edge_val.c.nodeB,
                table_edge_val.c.idx,
                table_edge_val.c.key,
                table_edge_val.c.branch,
                func.MAX(table_edge_val.c.rev).label('rev')
            ]
        ).where(
            and_(*hirev_where)
        ).group_by(
            table_edge_val.c.graph,
            table_edge_val.c.nodeA,
            table_edge_val.c.nodeB,
            table_edge_val.c.idx,
            table_edge_val.c.key,
            table_edge_val.c.branch
        ).alias('hirev')
        return select(
            [
                table_edge_val.c.graph,
                table_edge_val.c.nodeA,
                table_edge_val.c.nodeB,
                table_edge_val.c.idx,
                table_edge_val.c.key,
                table_edge_val.c.branch,
                table_edge_val.c.rev,
                table_edge_val.c.value
            ]
        ).select_from(
            table_edge_val.join(
                hirev_edge_val,
                and_(
                    table_edge_val.c.graph == hirev_edge_val.c.graph,
                    table_edge_val.c.nodeA == hirev_edge_val.c.nodeA,
                    table_edge_val.c.nodeB == hirev_edge_val.c.nodeB,
                    table_edge_val.c.idx == hirev_edge_val.c.idx,
                    table_edge_val.c.branch == hirev_edge_val.c.branch,
                    table_edge_val.c.rev == hirev_edge_val.c.rev
                )
            )
        )

    def edge_val_items(self, graph, nodeA, nodeB, idx, branch, rev):
        """Iterate over key-value pairs that are set on an edge as of
        ``(branch, rev)``

        """
        if not hasattr(self, '_edge_val_items_compiled'):
            evr = self._edge_val_recent()
            self._edge_val_items_compiled = select(
                [evr.c.key, evr.c.value]
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._edge_val_items_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            b=branch,
            r=rev
        )

    def edge_val_get(self, graph, nodeA, nodeB, idx, key, branch, rev):
        """Get the value of a key on an edge that is relevant as of ``(branch,
        rev)``

        """
        if not hasattr(self, '_edge_val_get_compiled'):
            evr = self._edge_val_recent(key=True)
            self._edge_val_get_compiled = select(
                [evr.c.value]
            ).where(
                evr.c.value != null()
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._edge_val_get_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            k=key,
            b=branch,
            r=rev
        )

    def edge_val_ins(self, graph, nodeA, nodeB, idx, key, branch, rev, value):
        """Insert a record to indicate the value of a key on an edge as of
        ``(branch, rev)``

        """
        if not hasattr(self, '_edge_val_ins_compiled'):
            self._edge_val_ins_compiled = table_edge_val.insert().values(
                graph=bindparam('g'),
                nodeA=bindparam('orig'),
                nodeB=bindparam('dest'),
                idx=bindparam('i'),
                key=bindparam('k'),
                branch=bindparam('b'),
                rev=bindparam('r'),
                value=bindparam('v')
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._edge_val_ins_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            k=key,
            b=branch,
            r=rev,
            v=value
        )

    def edge_val_upd(self, value, graph, nodeA, nodeB, idx, key, branch, rev):
        """Update a record previously inserted by ``edge_val_ins``"""
        if not hasattr(self, '_edge_val_upd_compiled'):
            self._edge_val_upd_compiled = table_edge_val.update().values(
                value=bindparam('v')
            ).where(
                and_(
                    table_edge_val.c.graph == bindparam('g'),
                    table_edge_val.c.nodeA == bindparam('orig'),
                    table_edge_val.c.nodeB == bindparam('dest'),
                    table_edge_val.c.idx == bindparam('i'),
                    table_edge_val.c.branch == bindparam('b'),
                    table_edge_val.c.rev == bindparam('r')
                )
            ).compile(dialect=self.engine.dialect)
        return self.conn.execute(
            self._edge_val_upd_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            k=key,
            b=branch,
            r=rev
        )

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
from sqlalchemy import create_engine
from json import dumps

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

table = {
    'global': table_global,
    'branches': table_branches,
    'graphs': table_graphs,
    'graph_val': table_graph_val,
    'nodes': table_nodes,
    'node_val': table_node_val,
    'edges': table_edges,
    'edge_val': table_edge_val
}


def compile_sql(dialect):
    def recent_nodes(node):
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

    rn_general = recent_nodes(False)
    rn_specific = recent_nodes(True)

    def recent_graph_val_join(key):
        """Return a query for the most recent graph_val
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
        return table_graph_val.join(
            hirev_graph_val,
            and_(
                table_graph_val.c.graph == hirev_graph_val.c.graph,
                table_graph_val.c.key == hirev_graph_val.c.key,
                table_graph_val.c.branch == hirev_graph_val.c.branch,
                table_graph_val.c.rev == hirev_graph_val.c.rev
            )
        )

    rgvj_general = recent_graph_val_join(False)
    rgvj_specific = recent_graph_val_join(True)

    def recent_node_val(key):
        """Return a query for getting the most recent value of
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

    rnv_general = recent_node_val(False)
    rnv_specific = recent_node_val(True)

    def edges_recent(orig=False, dest=False, idx=False):
        """Return a query to get the most recent edge
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

    edges_recent_default = edges_recent()
    edges_recent_dest = edges_recent(dest=True)
    edges_recent_orig = edges_recent(orig=True)
    edges_recent_multi = edges_recent(orig=True, dest=True)

    def edge_val_recent(key):
        """Make a query for getting the most recent record
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

    evr_general = edge_val_recent(False)
    evr_specific = edge_val_recent(True)

    return {
        'ctbranch': select(
            [func.COUNT(table_branches.c.branch)]
        ).where(
            table_branches.c.branch == bindparam('branch')
        ).compile(dialect=dialect),
        'ctgraph': select(
            [func.COUNT(table_graphs.c.graph)]
        ).where(
            table_graphs.c.graph == bindparam('graph')
        ).compile(dialect=dialect),
        'allbranch': select(
            [
                table_branches.c.branch,
                table_branches.c.parent,
                table_branches.c.parent_rev
            ]
        ).compile(dialect=dialect),
        'global_get': select(
            [table_global.c.value]
        ).where(
            table_global.c.key == bindparam('key')
        ).compile(dialect=dialect),
        'edge_val_ins': table_edge_val.insert().values(
            graph=bindparam('g'),
            nodeA=bindparam('orig'),
            nodeB=bindparam('dest'),
            idx=bindparam('i'),
            key=bindparam('k'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            value=bindparam('v')
        ).compile(dialect=dialect),
        'edge_val_upd': table_edge_val.update().values(
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
        ).compile(dialect=dialect),
        'global_items': select(
            [
                table_global.c.key,
                table_global.c.value
            ]
        ).compile(dialect=dialect),
        'ctglobal': select(
            [func.COUNT(table_global.c.key)]
        ).compile(dialect=dialect),
        'new_graph': table_graphs.insert().values(
            graph=bindparam('graph'),
            type=bindparam('type')
        ).compile(dialect=dialect),
        'graph_type': select(
            [table_graphs.c.type]
        ).where(
            table_graphs.c.graph == bindparam('g')
        ).compile(dialect=dialect),
        'new_branch': table_branches.insert().values(
            branch=bindparam('branch'),
            parent=bindparam('parent'),
            parent_rev=bindparam('parent_rev')
        ).compile(dialect=dialect),
        'del_edge_val_graph': table_edge_val.delete().where(
            table_edge_val.c.graph == bindparam('graph')
        ).compile(dialect=dialect),
        'del_node_val_graph': table_node_val.delete().where(
            table_node_val.c.graph == bindparam('graph')
        ).compile(dialect=dialect),
        'del_node_graph': table_nodes.delete().where(
            table_nodes.c.graph == bindparam('graph')
        ).compile(dialect=dialect),
        'del_graph': table_graphs.delete().where(
            table_graphs.c.graph == bindparam('graph')
        ).compile(dialect=dialect),
        'parrev': select(
            [table_branches.c.parent_rev]
        ).where(
            table_branches.c.branch == bindparam('branch')
        ).compile(dialect=dialect),
        'parparrev': select(
            [table_branches.c.parent, table_branches.c.parent_rev]
        ).where(
            table_branches.c.branch == bindparam('branch')
        ).compile(dialect=dialect),
        'global_ins': table_global.insert().values(
            key=bindparam('k'),
            value=bindparam('v')
        ).compile(dialect=dialect),
        'global_upd': table_global.update().values(
            value=bindparam('v')
        ).where(
            table_global.c.key == bindparam('k')
        ),
        'global_del': table_global.delete().where(
            table_global.c.key == bindparam('k')
        ).compile(dialect=dialect),
        'nodes_extant': select(
            [rn_general.c.node]
        ).where(
            rn_general.c.extant
        ).compile(dialect=dialect),
        'node_exists': select(
            [rn_specific.c.extant]
        ).compile(dialect=dialect),
        'exist_node_ins': table_nodes.insert().values(
            graph=bindparam('g'),
            node=bindparam('n'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            extant=bindparam('x')
        ).compile(dialect=dialect),
        'exist_node_upd': table_nodes.update().values(
            extant=bindparam('x')
        ).where(
            and_(
                table_nodes.c.graph == bindparam('g'),
                table_nodes.c.node == bindparam('n'),
                table_nodes.c.branch == bindparam('b'),
                table_nodes.c.rev == bindparam('r')
            )
        ).compile(dialect=dialect),
        'graph_val_items': select(
            [
                table_graph_val.c.key,
                table_graph_val.c.value
            ]
        ).select_from(
            rgvj_general
        ).compile(dialect=dialect),
        'graph_val_get': select(
            [
                table_graph_val.c.value
            ]
        ).select_from(
            rgvj_specific
        ).compile(dialect=dialect),
        'graph_val_ins': table_graph_val.insert().values(
            graph=bindparam('g'),
            key=bindparam('k'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            value=bindparam('v')
        ).compile(dialect=dialect),
        'graph_val_upd': table_graph_val.update().values(
            value=bindparam('v')
        ).where(
            and_(
                table_graph_val.c.graph == bindparam('g'),
                table_graph_val.c.key == bindparam('k'),
                table_graph_val.c.branch == bindparam('b'),
                table_graph_val.c.rev == bindparam('r')
            )
        ).compile(dialect=dialect),
        'node_val_items': select(
            [
                rnv_general.c.key,
                rnv_general.c.value
            ]
        ).compile(dialect=dialect),
        'node_val_get': select(
            [
                rnv_specific.c.value
            ]
        ).where(
            rnv_specific.c.value != null()
        ).compile(dialect=dialect),
        'node_val_ins': table_node_val.insert().values(
            graph=bindparam('g'),
            node=bindparam('n'),
            key=bindparam('k'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            value=bindparam('v')
        ).compile(dialect=dialect),
        'node_val_upd': table_node_val.update().values(
            value=bindparam('v')
        ).where(
            and_(
                table_node_val.c.graph == bindparam('g'),
                table_node_val.c.node == bindparam('n'),
                table_node_val.c.key == bindparam('k'),
                table_node_val.c.branch == bindparam('b'),
                table_node_val.c.rev == bindparam('r')
            )
        ).compile(dialect=dialect),
        'edge_exists': select(
            [edges_recent(orig=True, dest=True, idx=True).c.extant]
        ).compile(dialect=dialect),
        'edges_extant': select(
            [
                edges_recent_default.c.nodeA,
                edges_recent_default.c.extant
            ]
        ).compile(dialect=dialect),
        'nodeAs': select(
            [
                edges_recent_dest.c.nodeA,
                edges_recent_dest.c.extant
            ]
        ).compile(dialect=dialect),
        'nodeBs': select(
            [
                edges_recent_orig.c.nodeB,
                edges_recent_orig.c.extant
            ]
        ).compile(dialect=dialect),
        'multi_edges': select(
            [
                edges_recent_multi.c.idx,
                edges_recent_multi.c.extant
            ]
        ).compile(dialect=dialect),
        'edge_exist_ins': table_edges.insert().values(
            graph=bindparam('g'),
            nodeA=bindparam('orig'),
            nodeB=bindparam('dest'),
            idx=bindparam('i'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            extant=bindparam('x')
        ).compile(dialect=dialect),
        'edge_exist_upd': table_edges.update().values(
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
        ).compile(dialect=dialect),
        'edge_val_items': select(
            [
                evr_general.c.key,
                evr_general.c.value
            ]
        ).compile(dialect=dialect),
        'edge_val_get': select(
            [
                evr_specific.c.value
            ]
        ).where(
            evr_specific.c.value != null()
        ).compile(dialect=dialect)
    }


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
        self.sql = compile_sql(self.engine.dialect)

    def ctbranch(self, branch):
        """Query to count the number of branches that exist."""
        return self.conn.execute(
            self.sql['ctbranch'],
            branch=branch
        )

    def ctgraph(self, graph):
        """Query to count the number of graphs that have been created."""
        return self.conn.execute(
            self.sql['ctgraph'],
            graph=graph
        )

    def allbranch(self):
        """Iterate over all available branch data."""
        return self.conn.execute(
            self.sql['allbranch']
        )

    def global_get(self, key):
        """Get the value for a global key."""
        return self.conn.execute(
            self.sql['global_get'],
            key=key
        )

    def global_items(self):
        """Iterate over key-value pairs set globally."""
        return self.conn.execute(
            self.sql['global_items']
        )

    def ctglobal(self):
        """Count keys set globally."""
        return self.conn.execute(
            self.sql['ctglobal']
        )

    def new_graph(self, graph, typ):
        """Create a graph of a given type."""
        return self.conn.execute(
            self.sql['new_graph'],
            graph=graph,
            type=typ
        )

    def graph_type(self, graph):
        """Fetch the type of the named graph."""
        return self.conn.execute(
            self.sql['graph_type'],
            g=graph
        )

    def new_branch(self, branch, parent, parent_rev):
        """Declare that the branch ``branch`` is a child of ``parent``
        starting at revision ``parent_rev``.

        """
        return self.conn.execute(
            self.sql['new_branch'],
            branch=branch,
            parent=parent,
            parent_rev=parent_rev
        )

    def del_edge_val_graph(self, graph):
        """Delete all edge attributes from ``graph``."""
        return self.conn.execute(
            self.sql['del_edge_val_graph'],
            graph=graph
        )

    def del_node_val_graph(self, graph):
        """Delete all node attributes from ``graph``."""
        return self.conn.execute(
            self.sql['del_node_val_graph'],
            graph=graph
        )

    def del_node_graph(self, graph):
        """Delete all nodes from ``graph``."""
        return self.conn.execute(
            self.sql['del_node_graph'],
            graph=graph
        )

    def del_graph(self, graph):
        """Delete the graph header."""
        return self.conn.execute(
            self.sql['del_graph'],
            graph=graph
        )

    def parrev(self, branch):
        """Fetch the revision at which ``branch`` forks off from its
        parent.

        """
        return self.conn.execute(
            self.sql['parrev'],
            branch=branch
        )

    def parparrev(self, branch):
        """Fetch the name of ``branch``'s parent, and the revision at which
        they part.

        """
        return self.conn.execute(
            self.sql['parparrev'],
            branch=branch
        )

    def global_ins(self, key, value):
        """Insert a record into the globals table indicating that
        ``key=value``.

        """
        return self.conn.execute(
            self.sql['global_ins'],
            k=key,
            v=value
        )

    def global_upd(self, key, value):
        """Update the existing global record for ``key`` so that it is set to
        ``value``.

        """
        return self.conn.execute(
            self.sql['global_upd'],
            k=key,
            v=value
        )

    def global_del(self, key):
        """Delete the record for global variable ``key``."""
        return self.conn.execute(
            self.sql['global_del'],
            k=key
        )

    def nodes_extant(self, graph, branch, rev):
        """Query for nodes that exist in ``graph`` at ``(branch, rev)``."""
        return self.conn.execute(
            self.sql['nodes_extant'],
            g=graph,
            b=branch,
            r=rev
        )

    def node_exists(self, graph, node, branch, rev):
        """Query for whether or not ``node`` exists in ``graph`` at ``(branch,
        rev)``.

        """
        return self.conn.execute(
            self.sql['node_exists'],
            g=graph,
            n=node,
            b=branch,
            r=rev
        )

    def exist_node_ins(self, graph, node, branch, rev, extant):
        """Insert a record to indicate whether or not ``node`` exists in
        ``graph`` at ``(branch, rev)``.

        """
        return self.conn.execute(
            self.sql['exist_node_ins'],
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
        return self.conn.execute(
            self.sql['exist_node_upd'],
            x=extant,
            g=graph,
            n=node,
            b=branch,
            r=rev
        )

    def graph_val_items(self, graph, branch, rev):
        """Query the most recent keys and values for the attributes of
        ``graph`` at ``(branch, rev)``.

        """
        return self.conn.execute(
            self.sql['graph_val_items'],
            g=graph,
            b=branch,
            r=rev
        )

    def graph_val_get(self, graph, key, branch, rev):
        """Query the most recent value for ``graph``'s ``key`` as of
        ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['graph_val_get'],
            g=graph,
            k=key,
            b=branch,
            r=rev
        )

    def graph_val_ins(self, graph, key, branch, rev, value):
        """Insert a record to indicate that ``key=value`` on ``graph`` as of
        ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['graph_val_ins'],
            g=graph,
            k=key,
            b=branch,
            r=rev,
            v=value
        )

    def graph_val_upd(self, value, graph, key, branch, rev):
        """Update the record previously inserted by ``graph_val_ins``"""
        return self.conn.execute(
            self.sql['graph_val_upd'],
            v=value,
            g=graph,
            k=key,
            b=branch,
            r=rev
        )

    def node_val_items(self, graph, node, branch, rev):
        """Get all the most recent values of all the keys on ``node`` in
        ``graph`` as of ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['node_val_items'],
            g=graph,
            n=node,
            b=branch,
            r=rev
        )

    def node_val_get(self, graph, node, key, branch, rev):
        """Get the most recent value for ``key`` on ``node`` in ``graph`` as
        of ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['node_val_get'],
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
        return self.conn.execute(
            self.sql['node_val_ins'],
            g=graph,
            n=node,
            k=key,
            b=branch,
            r=rev,
            v=value
        )

    def node_val_upd(self, value, graph, node, key, branch, rev):
        """Update the record previously inserted by ``node_val_ins``"""
        return self.conn.execute(
            self.sql['node_val_upd'],
            v=value,
            g=graph,
            n=node,
            k=key,
            b=branch,
            r=rev
        )

    def edge_exists(self, graph, nodeA, nodeB, idx, branch, rev):
        """Query for whether a particular edge exists at a particular
        ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['edge_exists'],
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
        return self.conn.execute(
            self.sql['edges_extant'],
            g=graph,
            b=branch,
            r=rev
        )

    def nodeAs(self, graph, nodeB, branch, rev):
        """Query for edges that end at ``nodeB`` in ``graph`` as of ``(branch,
        rev)``

        """
        return self.conn.execute(
            self.sql['nodeAs'],
            g=graph,
            dest=nodeB,
            b=branch,
            r=rev
        )

    def nodeBs(self, graph, nodeA, branch, rev):
        """Query for the nodes at which edges that originate from ``nodeA``
        end.

        """
        return self.conn.execute(
            self.sql['nodeBs'],
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
        return self.conn.execute(
            self.sql['multi_edges'],
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
        return self.conn.execute(
            self.sql['edge_exist_ins'],
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
        return self.conn.execute(
            self.sql['edge_exist_upd'],
            x=extant,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            b=branch,
            r=rev
        )

    def edge_val_items(self, graph, nodeA, nodeB, idx, branch, rev):
        """Iterate over key-value pairs that are set on an edge as of
        ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['edge_val_items'],
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
        return self.conn.execute(
            self.sql['edge_val_get'],
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
        return self.conn.execute(
            self.sql['edge_val_ins'],
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
        return self.conn.execute(
            self.sql['edge_val_upd'],
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            k=key,
            b=branch,
            r=rev
        )


if __name__ == '__main__':
    e = create_engine('sqlite://')
    print(
        dumps(
            dict(
                (k, str(v)) for (k, v) in
                compile_sql(e.dialect).items()
            )
        )
    )

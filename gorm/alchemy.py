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
    DateTime,
    MetaData,
    ForeignKey,
    select,
    func,
    and_,
    null
)
from sqlalchemy.sql import bindparam
from sqlalchemy.sql.ddl import CreateTable, CreateIndex
from sqlalchemy import create_engine
from json import dumps

length = 50

TEXT = String(length)


def tables_for_meta(meta):
    return {
        'global': Table(
            'global', meta,
            Column('key', TEXT, primary_key=True),
            Column('date', DateTime, nullable=True),
            Column('creator', TEXT, nullable=True),
            Column('description', TEXT, nullable=True),
            Column('value', TEXT, nullable=True)
        ),
        'branches': Table(
            'branches', meta,
            Column(
                'branch', TEXT, ForeignKey('branches.parent'),
                primary_key=True, default='master'
            ),
            Column('date', DateTime, nullable=True),
            Column('creator', TEXT, nullable=True),
            Column('description', TEXT, nullable=True),
            Column('parent', TEXT, default='master'),
            Column('parent_rev', Integer, default=0)
        ),
        'graphs': Table(
            'graphs', meta,
            Column('graph', TEXT, primary_key=True),
            Column('date', DateTime, nullable=True),
            Column('creator', TEXT, nullable=True),
            Column('description', TEXT, nullable=True),
            Column('type', TEXT, default='Graph'),
            CheckConstraint(
                "type IN ('Graph', 'DiGraph', 'MultiGraph', 'MultiDiGraph')"
            )
        ),
        'graph_val': Table(
            'graph_val', meta,
            Column('graph', TEXT, ForeignKey('graphs.graph'),
                   primary_key=True),
            Column('key', TEXT, primary_key=True),
            Column('branch', TEXT, ForeignKey('branches.branch'),
                   primary_key=True, default='master'),
            Column('rev', Integer, primary_key=True, default=0),
            Column('date', DateTime, nullable=True),
            Column('contributor', TEXT, nullable=True),
            Column('description', TEXT, nullable=True),
            Column('value', TEXT, nullable=True)
        ),
        'nodes': Table(
            'nodes', meta,
            Column('graph', TEXT, ForeignKey('graphs.graph'),
                   primary_key=True),
            Column('node', TEXT, primary_key=True),
            Column('branch', TEXT, ForeignKey('branches.branch'),
                   primary_key=True, default='master'),
            Column('rev', Integer, primary_key=True, default=0),
            Column('date', DateTime, nullable=True),
            Column('creator', TEXT, nullable=True),
            Column('description', TEXT, nullable=True),
            Column('extant', Boolean)
        ),
        'node_val': Table(
            'node_val', meta,
            Column('graph', TEXT, primary_key=True),
            Column('node', TEXT, primary_key=True),
            Column('key', TEXT, primary_key=True),
            Column('branch', TEXT, ForeignKey('branches.branch'),
                   primary_key=True, default='master'),
            Column('rev', Integer, primary_key=True, default=0),
            Column('date', DateTime, nullable=True),
            Column('contributor', TEXT, nullable=True),
            Column('description', TEXT, nullable=True),
            Column('value', TEXT, nullable=True),
            ForeignKeyConstraint(
                ['graph', 'node'], ['nodes.graph', 'nodes.node']
            )
        ),
        'edges': Table(
            'edges', meta,
            Column('graph', TEXT, ForeignKey('graphs.graph'),
                   primary_key=True),
            Column('nodeA', TEXT, primary_key=True),
            Column('nodeB', TEXT, primary_key=True),
            Column('idx', Integer, primary_key=True),
            Column('branch', TEXT, ForeignKey('branches.branch'),
                   primary_key=True, default='master'),
            Column('rev', Integer, primary_key=True, default=0),
            Column('date', DateTime, nullable=True),
            Column('creator', TEXT, nullable=True),
            Column('description', TEXT, nullable=True),
            Column('extant', Boolean),
            ForeignKeyConstraint(
                ['graph', 'nodeA'], ['nodes.graph', 'nodes.node']
            ),
            ForeignKeyConstraint(
                ['graph', 'nodeB'], ['nodes.graph', 'nodes.node']
            )
        ),
        'edge_val': Table(
            'edge_val', meta,
            Column('graph', TEXT, primary_key=True),
            Column('nodeA', TEXT, primary_key=True),
            Column('nodeB', TEXT, primary_key=True),
            Column('idx', Integer, primary_key=True),
            Column('key', TEXT, primary_key=True),
            Column('branch', TEXT, ForeignKey('branches.branch'),
                   primary_key=True, default='master'),
            Column('rev', Integer, primary_key=True, default=0),
            Column('date', DateTime, nullable=True),
            Column('contributor', TEXT, nullable=True),
            Column('description', TEXT, nullable=True),
            Column('value', TEXT, nullable=True),
            ForeignKeyConstraint(
                ['graph', 'nodeA', 'nodeB', 'idx'],
                ['edges.graph', 'edges.nodeA', 'edges.nodeB', 'edges.idx']
            )
        )
    }


def indices_for_table_dict(table):
    return {
        'graph_val': Index(
            "graph_val_idx",
            table['graph_val'].c.graph,
            table['graph_val'].c.key
        ),
        'nodes': Index(
            "nodes_idx",
            table['nodes'].c.graph,
            table['nodes'].c.node
        ),
        'node_val': Index(
            "node_val_idx",
            table['node_val'].c.graph,
            table['node_val'].c.node
        ),
        'edges': Index(
            "edges_idx",
            table['edges'].c.graph,
            table['edges'].c.nodeA,
            table['edges'].c.nodeB,
            table['edges'].c.idx
        ),
        'edge_val': Index(
            "edge_val_idx",
            table['edge_val'].c.graph,
            table['edge_val'].c.nodeA,
            table['edge_val'].c.nodeB,
            table['edge_val'].c.idx,
            table['edge_val'].c.key
        )
    }


def queries_for_table_dict(table):
    def hirev_nodes_join(wheres):
        hirev = select(
            [
                table['nodes'].c.graph,
                table['nodes'].c.node,
                table['nodes'].c.branch,
                func.MAX(table['nodes'].c.rev).label('rev')
            ]
        ).where(and_(*wheres)).group_by(
            table['nodes'].c.graph,
            table['nodes'].c.node,
            table['nodes'].c.branch
        ).alias('hirev')
        return table['nodes'].join(
            hirev,
            and_(
                table['nodes'].c.graph == hirev.c.graph,
                table['nodes'].c.node == hirev.c.node,
                table['nodes'].c.branch == hirev.c.branch,
                table['nodes'].c.rev == hirev.c.rev
            )
        )

    def hirev_graph_val_join(wheres):
        hirev = select(
            [
                table['graph_val'].c.graph,
                table['graph_val'].c.key,
                table['graph_val'].c.branch,
                func.MAX(table['graph_val'].c.rev).label('rev')
            ]
        ).where(and_(*wheres)).group_by(
            table['graph_val'].c.graph,
            table['graph_val'].c.key,
            table['graph_val'].c.branch
        ).alias('hirev')
        return table['graph_val'].join(
            hirev,
            and_(
                table['graph_val'].c.graph == hirev.c.graph,
                table['graph_val'].c.key == hirev.c.key,
                table['graph_val'].c.branch == hirev.c.branch,
                table['graph_val'].c.rev == hirev.c.rev
            )
        )

    def node_val_hirev_join(wheres):
        hirev = select(
            [
                table['node_val'].c.graph,
                table['node_val'].c.node,
                table['node_val'].c.branch,
                table['node_val'].c.key,
                func.MAX(table['node_val'].c.rev).label('rev')
            ]
        ).where(and_(*wheres)).group_by(
            table['node_val'].c.graph,
            table['node_val'].c.node,
            table['node_val'].c.branch,
            table['node_val'].c.key
        ).alias('hirev')

        return table['node_val'].join(
            hirev,
            and_(
                table['node_val'].c.graph == hirev.c.graph,
                table['node_val'].c.node == hirev.c.node,
                table['node_val'].c.key == hirev.c.key,
                table['node_val'].c.branch == hirev.c.branch,
                table['node_val'].c.rev == hirev.c.rev
            )
        )

    def edges_recent_join(wheres=None):
        hirev = select(
            [
                table['edges'].c.graph,
                table['edges'].c.nodeA,
                table['edges'].c.nodeB,
                table['edges'].c.idx,
                table['edges'].c.branch,
                func.MAX(table['edges'].c.rev).label('rev')
            ]
        )
        if wheres:
            hirev = hirev.where(and_(*wheres))
        hirev = hirev.group_by(
            table['edges'].c.graph,
            table['edges'].c.nodeA,
            table['edges'].c.nodeB,
            table['edges'].c.idx,
            table['edges'].c.branch
        ).alias('hirev')
        return table['edges'].join(
            hirev,
            and_(
                table['edges'].c.graph == hirev.c.graph,
                table['edges'].c.nodeA == hirev.c.nodeA,
                table['edges'].c.nodeB == hirev.c.nodeB,
                table['edges'].c.idx == hirev.c.idx,
                table['edges'].c.branch == hirev.c.branch,
                table['edges'].c.rev == hirev.c.rev
            )
        )

    def edge_val_recent_join(wheres=None):
        hirev = select(
            [
                table['edge_val'].c.graph,
                table['edge_val'].c.nodeA,
                table['edge_val'].c.nodeB,
                table['edge_val'].c.idx,
                table['edge_val'].c.key,
                table['edge_val'].c.branch,
                func.MAX(table['edge_val'].c.rev).label('rev')
            ]
        )
        if wheres:
            hirev = hirev.where(
                and_(*wheres)
            )
        hirev = hirev.group_by(
            table['edge_val'].c.graph,
            table['edge_val'].c.nodeA,
            table['edge_val'].c.nodeB,
            table['edge_val'].c.idx,
            table['edge_val'].c.key,
            table['edge_val'].c.branch
        ).alias('hirev')
        return table['edge_val'].join(
            hirev,
            and_(
                table['edge_val'].c.graph == hirev.c.graph,
                table['edge_val'].c.nodeA == hirev.c.nodeA,
                table['edge_val'].c.nodeB == hirev.c.nodeB,
                table['edge_val'].c.idx == hirev.c.idx,
                table['edge_val'].c.branch == hirev.c.branch,
                table['edge_val'].c.rev == hirev.c.rev
            )
        )

    return {
        'ctbranch': select(
            [func.COUNT(table['branches'].c.branch)]
        ).where(
            table['branches'].c.branch == bindparam('branch')
        ),
        'ctgraph': select(
            [func.COUNT(table['graphs'].c.graph)]
        ).where(
            table['graphs'].c.graph == bindparam('graph')
        ),
        'allbranch': select(
            [
                table['branches'].c.branch,
                table['branches'].c.parent,
                table['branches'].c.parent_rev
            ]
        ),
        'global_get': select(
            [table['global'].c.value]
        ).where(
            table['global'].c.key == bindparam('key')
        ),
        'edge_val_ins': table['edge_val'].insert().values(
            graph=bindparam('graph'),
            nodeA=bindparam('orig'),
            nodeB=bindparam('dest'),
            idx=bindparam('idx'),
            key=bindparam('key'),
            branch=bindparam('branch'),
            rev=bindparam('rev'),
            value=bindparam('value')
        ),
        'edge_val_upd': table['edge_val'].update().values(
            value=bindparam('value')
        ).where(
            and_(
                table['edge_val'].c.graph == bindparam('graph'),
                table['edge_val'].c.nodeA == bindparam('orig'),
                table['edge_val'].c.nodeB == bindparam('dest'),
                table['edge_val'].c.idx == bindparam('idx'),
                table['edge_val'].c.branch == bindparam('branch'),
                table['edge_val'].c.rev == bindparam('rev')
            )
        ),
        'global_items': select(
            [
                table['global'].c.key,
                table['global'].c.value
            ]
        ),
        'ctglobal': select(
            [func.COUNT(table['global'].c.key)]
        ),
        'new_graph': table['graphs'].insert().values(
            graph=bindparam('graph'),
            type=bindparam('type')
        ),
        'graph_type': select(
            [table['graphs'].c.type]
        ).where(
            table['graphs'].c.graph == bindparam('graph')
        ),
        'new_branch': table['branches'].insert().values(
            branch=bindparam('branch'),
            parent=bindparam('parent'),
            parent_rev=bindparam('parent_rev')
        ),
        'del_edge_val_graph': table['edge_val'].delete().where(
            table['edge_val'].c.graph == bindparam('graph')
        ),
        'del_node_val_graph': table['node_val'].delete().where(
            table['node_val'].c.graph == bindparam('graph')
        ),
        'del_node_graph': table['nodes'].delete().where(
            table['nodes'].c.graph == bindparam('graph')
        ),
        'del_graph': table['graphs'].delete().where(
            table['graphs'].c.graph == bindparam('graph')
        ),
        'parrev': select(
            [table['branches'].c.parent_rev]
        ).where(
            table['branches'].c.branch == bindparam('branch')
        ),
        'parparrev': select(
            [table['branches'].c.parent, table['branches'].c.parent_rev]
        ).where(
            table['branches'].c.branch == bindparam('branch')
        ),
        'global_ins': table['global'].insert().values(
            key=bindparam('key'),
            value=bindparam('value')
        ),
        'global_upd': table['global'].update().values(
            value=bindparam('value')
        ).where(
            table['global'].c.key == bindparam('key')
        ),
        'global_del': table['global'].delete().where(
            table['global'].c.key == bindparam('key')
        ),
        'nodes_extant': select(
            [table['nodes'].c.node]
        ).select_from(
            hirev_nodes_join(
                [
                    table['nodes'].c.graph == bindparam('graph'),
                    table['nodes'].c.branch == bindparam('branch'),
                    table['nodes'].c.rev <= bindparam('rev')
                ]
            )
        ).where(
            table['nodes'].c.extant
        ),
        'node_exists': select(
            [table['nodes'].c.extant]
        ).select_from(
            hirev_nodes_join(
                [
                    table['nodes'].c.graph == bindparam('graph'),
                    table['nodes'].c.node == bindparam('node'),
                    table['nodes'].c.branch == bindparam('branch'),
                    table['nodes'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'exist_node_ins': table['nodes'].insert().values(
            graph=bindparam('graph'),
            node=bindparam('node'),
            branch=bindparam('branch'),
            rev=bindparam('rev'),
            extant=bindparam('extant')
        ),
        'exist_node_upd': table['nodes'].update().values(
            extant=bindparam('extant')
        ).where(
            and_(
                table['nodes'].c.graph == bindparam('graph'),
                table['nodes'].c.node == bindparam('node'),
                table['nodes'].c.branch == bindparam('branch'),
                table['nodes'].c.rev == bindparam('rev')
            )
        ),
        'nodes_dump': select([
            table['nodes'].c.graph,
            table['nodes'].c.node,
            table['nodes'].c.branch,
            table['nodes'].c.rev,
            table['nodes'].c.extant
        ]),
        'graph_val_items': select(
            [
                table['graph_val'].c.key,
                table['graph_val'].c.value
            ]
        ).select_from(
            hirev_graph_val_join(
                [
                    table['graph_val'].c.graph == bindparam('graph'),
                    table['graph_val'].c.branch == bindparam('branch'),
                    table['graph_val'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'graph_val_dump': select([
            table['graph_val'].c.graph,
            table['graph_val'].c.key,
            table['graph_val'].c.branch,
            table['graph_val'].c.rev,
            table['graph_val'].c.value
        ]),
        'graph_val_get': select(
            [
                table['graph_val'].c.value
            ]
        ).select_from(
            hirev_graph_val_join(
                [
                    table['graph_val'].c.graph == bindparam('graph'),
                    table['graph_val'].c.key == bindparam('key'),
                    table['graph_val'].c.branch == bindparam('branch'),
                    table['graph_val'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'graph_val_ins': table['graph_val'].insert().values(
            graph=bindparam('graph'),
            key=bindparam('key'),
            branch=bindparam('branch'),
            rev=bindparam('rev'),
            value=bindparam('value')
        ),
        'graph_val_upd': table['graph_val'].update().values(
            value=bindparam('value')
        ).where(
            and_(
                table['graph_val'].c.graph == bindparam('graph'),
                table['graph_val'].c.key == bindparam('key'),
                table['graph_val'].c.branch == bindparam('branch'),
                table['graph_val'].c.rev == bindparam('rev')
            )
        ),
        'node_val_items': select(
            [
                table['node_val'].c.key,
                table['node_val'].c.value
            ]
        ).select_from(
            node_val_hirev_join(
                [
                    table['node_val'].c.graph == bindparam('graph'),
                    table['node_val'].c.node == bindparam('node'),
                    table['node_val'].c.branch == bindparam('branch'),
                    table['node_val'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'node_val_dump': select([
            table['node_val'].c.graph,
            table['node_val'].c.node,
            table['node_val'].c.key,
            table['node_val'].c.branch,
            table['node_val'].c.rev,
            table['node_val'].c.value
        ]),
        'node_val_get': select(
            [
                table['node_val'].c.value
            ]
        ).select_from(
            node_val_hirev_join(
                [
                    table['node_val'].c.graph == bindparam('graph'),
                    table['node_val'].c.node == bindparam('node'),
                    table['node_val'].c.key == bindparam('key'),
                    table['node_val'].c.branch == bindparam('branch'),
                    table['node_val'].c.rev <= bindparam('rev')
                ]
            )
        ).where(
            table['node_val'].c.value != null()
        ),
        'node_val_ins': table['node_val'].insert().values(
            graph=bindparam('graph'),
            node=bindparam('node'),
            key=bindparam('key'),
            branch=bindparam('branch'),
            rev=bindparam('rev'),
            value=bindparam('value')
        ),
        'node_val_upd': table['node_val'].update().values(
            value=bindparam('value')
        ).where(
            and_(
                table['node_val'].c.graph == bindparam('graph'),
                table['node_val'].c.node == bindparam('node'),
                table['node_val'].c.key == bindparam('key'),
                table['node_val'].c.branch == bindparam('branch'),
                table['node_val'].c.rev == bindparam('rev')
            )
        ),
        'edge_exists': select(
            [table['edges'].c.extant]
        ).select_from(
            edges_recent_join(
                [
                    table['edges'].c.graph == bindparam('graph'),
                    table['edges'].c.nodeA == bindparam('nodeA'),
                    table['edges'].c.nodeB == bindparam('nodeB'),
                    table['edges'].c.idx == bindparam('idx'),
                    table['edges'].c.branch == bindparam('branch'),
                    table['edges'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'edges_extant': select(
            [
                table['edges'].c.nodeA,
                table['edges'].c.extant
            ]
        ).select_from(
            edges_recent_join(
                [
                    table['edges'].c.graph == bindparam('graph'),
                    table['edges'].c.branch == bindparam('branch'),
                    table['edges'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'nodeAs': select(
            [
                table['edges'].c.nodeA,
                table['edges'].c.extant
            ]
        ).select_from(
            edges_recent_join(
                [
                    table['edges'].c.graph == bindparam('graph'),
                    table['edges'].c.nodeB == bindparam('dest'),
                    table['edges'].c.branch == bindparam('branch'),
                    table['edges'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'nodeBs': select(
            [
                table['edges'].c.nodeB,
                table['edges'].c.extant
            ]
        ).select_from(
            edges_recent_join(
                [
                    table['edges'].c.graph == bindparam('graph'),
                    table['edges'].c.nodeA == bindparam('orig'),
                    table['edges'].c.branch == bindparam('branch'),
                    table['edges'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'multi_edges': select(
            [
                table['edges'].c.idx,
                table['edges'].c.extant
            ]
        ).select_from(
            edges_recent_join(
                [
                    table['edges'].c.graph == bindparam('graph'),
                    table['edges'].c.nodeA == bindparam('orig'),
                    table['edges'].c.nodeB == bindparam('dest'),
                    table['edges'].c.branch == bindparam('branch'),
                    table['edges'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'edges_dump': select([
            table['edges'].c.graph,
            table['edges'].c.nodeA,
            table['edges'].c.nodeB,
            table['edges'].c.idx,
            table['edges'].c.branch,
            table['edges'].c.rev,
            table['edges'].c.extant
        ]),
        'edge_exist_ins': table['edges'].insert().values(
            graph=bindparam('graph'),
            nodeA=bindparam('orig'),
            nodeB=bindparam('dest'),
            idx=bindparam('idx'),
            branch=bindparam('branch'),
            rev=bindparam('rev'),
            extant=bindparam('extant')
        ),
        'edge_exist_upd': table['edges'].update().values(
            extant=bindparam('extant')
        ).where(
            and_(
                table['edges'].c.graph == bindparam('graph'),
                table['edges'].c.nodeA == bindparam('orig'),
                table['edges'].c.nodeB == bindparam('dest'),
                table['edges'].c.idx == bindparam('idx'),
                table['edges'].c.branch == bindparam('branch'),
                table['edges'].c.rev == bindparam('rev')
            )
        ),
        'edge_val_dump': select([
            table['edge_val'].c.graph,
            table['edge_val'].c.nodeA,
            table['edge_val'].c.nodeB,
            table['edge_val'].c.idx,
            table['edge_val'].c.key,
            table['edge_val'].c.branch,
            table['edge_val'].c.rev,
            table['edge_val'].c.value
        ]),
        'edge_val_items': select(
            [
                table['edge_val'].c.key,
                table['edge_val'].c.value
            ]
        ).select_from(
            edge_val_recent_join(
                [
                    table['edge_val'].c.graph == bindparam('graph'),
                    table['edge_val'].c.nodeA == bindparam('orig'),
                    table['edge_val'].c.nodeB == bindparam('dest'),
                    table['edge_val'].c.idx == bindparam('idx'),
                    table['edge_val'].c.branch == bindparam('branch'),
                    table['edge_val'].c.rev <= bindparam('rev')
                ]
            )
        ),
        'edge_val_get': select(
            [
                table['edge_val'].c.value
            ]
        ).select_from(
            edge_val_recent_join(
                [
                    table['edge_val'].c.graph == bindparam('graph'),
                    table['edge_val'].c.nodeA == bindparam('orig'),
                    table['edge_val'].c.nodeB == bindparam('dest'),
                    table['edge_val'].c.idx == bindparam('idx'),
                    table['edge_val'].c.key == bindparam('key'),
                    table['edge_val'].c.branch == bindparam('branch'),
                    table['edge_val'].c.rev <= bindparam('rev')
                ]
            )
        )
    }


def compile_sql(dialect, meta):
    r = {}
    table = tables_for_meta(meta)
    index = indices_for_table_dict(table)
    query = queries_for_table_dict(table)

    for t in table.values():
        r['create_' + t.name] = CreateTable(t).compile(dialect=dialect)
    for (tab, idx) in index.items():
        r['index_' + tab] = CreateIndex(idx).compile(dialect=dialect)
    for (name, q) in query.items():
        r[name] = q.compile(dialect=dialect)

    return r


class Alchemist(object):
    """Holds an engine and runs queries on it.

    """
    def __init__(self, engine):
        self.engine = engine
        self.conn = self.engine.connect()
        self.meta = MetaData()
        self.sql = compile_sql(self.engine.dialect, self.meta)

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
            graph=graph
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
            key=key,
            value=value
        )

    def global_upd(self, key, value):
        """Update the existing global record for ``key`` so that it is set to
        ``value``.

        """
        return self.conn.execute(
            self.sql['global_upd'],
            key=key,
            value=value
        )

    def global_del(self, key):
        """Delete the record for global variable ``key``."""
        return self.conn.execute(
            self.sql['global_del'],
            key=key
        )

    def nodes_extant(self, graph, branch, rev):
        """Query for nodes that exist in ``graph`` at ``(branch, rev)``."""
        return self.conn.execute(
            self.sql['nodes_extant'],
            graph=graph,
            branch=branch,
            rev=rev
        )

    def node_exists(self, graph, node, branch, rev):
        """Query for whether or not ``node`` exists in ``graph`` at ``(branch,
        rev)``.

        """
        return self.conn.execute(
            self.sql['node_exists'],
            graph=graph,
            node=node,
            branch=branch,
            rev=rev
        )

    def exist_node_ins(self, graph, node, branch, rev, extant):
        """Insert a record to indicate whether or not ``node`` exists in
        ``graph`` at ``(branch, rev)``.

        """
        return self.conn.execute(
            self.sql['exist_node_ins'],
            graph=graph,
            node=node,
            branch=branch,
            rev=rev,
            extant=extant
        )

    def exist_node_upd(self, extant, graph, node, branch, rev):
        """Update the record previously inserted by ``exist_node_ins``,
        indicating whether ``node`` exists in ``graph`` at ``(branch,
        rev)``.

        """
        return self.conn.execute(
            self.sql['exist_node_upd'],
            extant=extant,
            graph=graph,
            node=node,
            branch=branch,
            rev=rev
        )

    def graph_val_items(self, graph, branch, rev):
        """Query the most recent keys and values for the attributes of
        ``graph`` at ``(branch, rev)``.

        """
        return self.conn.execute(
            self.sql['graph_val_items'],
            graph=graph,
            branch=branch,
            rev=rev
        )

    def graph_val_get(self, graph, key, branch, rev):
        """Query the most recent value for ``graph``'s ``key`` as of
        ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['graph_val_get'],
            graph=graph,
            key=key,
            branch=branch,
            rev=rev,
        )

    def graph_val_ins(self, graph, key, branch, rev, value):
        """Insert a record to indicate that ``key=value`` on ``graph`` as of
        ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['graph_val_ins'],
            graph=graph,
            key=key,
            branch=branch,
            rev=rev,
            value=value
        )

    def graph_val_upd(self, value, graph, key, branch, rev):
        """Update the record previously inserted by ``graph_val_ins``"""
        return self.conn.execute(
            self.sql['graph_val_upd'],
            value=value,
            graph=graph,
            key=key,
            branch=branch,
            rev=rev
        )

    def node_val_items(self, graph, node, branch, rev):
        """Get all the most recent values of all the keys on ``node`` in
        ``graph`` as of ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['node_val_items'],
            graph=graph,
            node=node,
            branch=branch,
            rev=rev
        )

    def node_val_get(self, graph, node, key, branch, rev):
        """Get the most recent value for ``key`` on ``node`` in ``graph`` as
        of ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['node_val_get'],
            graph=graph,
            node=node,
            key=key,
            branch=branch,
            rev=rev
        )

    def node_val_ins(self, graph, node, key, branch, rev, value):
        """Insert a record to indicate that the value of ``key`` on ``node``
        in ``graph`` as of ``(branch, rev)`` is ``value``.

        """
        return self.conn.execute(
            self.sql['node_val_ins'],
            graph=graph,
            node=node,
            key=key,
            branch=branch,
            rev=rev,
            value=value
        )

    def node_val_upd(self, value, graph, node, key, branch, rev):
        """Update the record previously inserted by ``node_val_ins``"""
        return self.conn.execute(
            self.sql['node_val_upd'],
            value=value,
            graph=graph,
            node=node,
            key=key,
            branch=branch,
            rev=rev
        )

    def edge_exists(self, graph, nodeA, nodeB, idx, branch, rev):
        """Query for whether a particular edge exists at a particular
        ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['edge_exists'],
            graph=graph,
            orig=nodeA,
            dest=nodeB,
            idx=idx,
            branch=branch,
            rev=rev
        )

    def edges_extant(self, graph, branch, rev):
        """Query for all edges that exist in ``graph`` as of ``(branch,
        rev)``

        """
        return self.conn.execute(
            self.sql['edges_extant'],
            graph=graph,
            branch=branch,
            rev=rev
        )

    def nodeAs(self, graph, nodeB, branch, rev):
        """Query for edges that end at ``nodeB`` in ``graph`` as of ``(branch,
        rev)``

        """
        return self.conn.execute(
            self.sql['nodeAs'],
            graph=graph,
            dest=nodeB,
            branch=branch,
            rev=rev
        )

    def nodeBs(self, graph, nodeA, branch, rev):
        """Query for the nodes at which edges that originate from ``nodeA``
        end.

        """
        return self.conn.execute(
            self.sql['nodeBs'],
            graph=graph,
            orig=nodeA,
            branch=branch,
            rev=rev
        )

    def multi_edges(self, graph, nodeA, nodeB, branch, rev):
        """Query for all edges from ``nodeA`` to ``nodeB``. Only makes sense
        if we're dealing with a :class:`MultiGraph` or
        :class:`MultiDiGraph`.

        """
        return self.conn.execute(
            self.sql['multi_edges'],
            graph=graph,
            orig=nodeA,
            dest=nodeB,
            branch=branch,
            rev=rev
        )

    def edge_exist_ins(self, graph, nodeA, nodeB, idx, branch, rev, extant):
        """Indicate that there is (or isn't) an edge from ``nodeA`` to
        ``nodeB`` in ``graph`` as of ``(branch, rev)``.

        ``idx`` should be ``0`` unless ``graph`` is a
        :class:`MultiGraph` or :class:`MultiDiGraph`.

        """
        return self.conn.execute(
            self.sql['edge_exist_ins'],
            graph=graph,
            orig=nodeA,
            dest=nodeB,
            idx=idx,
            branch=branch,
            rev=rev,
            extant=extant
        )

    def edge_exist_upd(self, extant, graph, nodeA, nodeB, idx, branch, rev):
        """Update a record previously inserted with ``edge_exist_ins``."""
        return self.conn.execute(
            self.sql['edge_exist_upd'],
            extant=extant,
            graph=graph,
            orig=nodeA,
            dest=nodeB,
            idx=idx,
            branch=branch,
            rev=rev
        )

    def edge_val_items(self, graph, nodeA, nodeB, idx, branch, rev):
        """Iterate over key-value pairs that are set on an edge as of
        ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['edge_val_items'],
            graph=graph,
            orig=nodeA,
            dest=nodeB,
            idx=idx,
            branch=branch,
            rev=rev
        )

    def edge_val_get(self, graph, nodeA, nodeB, idx, key, branch, rev):
        """Get the value of a key on an edge that is relevant as of ``(branch,
        rev)``

        """
        return self.conn.execute(
            self.sql['edge_val_get'],
            graph=graph,
            orig=nodeA,
            dest=nodeB,
            idx=idx,
            key=key,
            branch=branch,
            rev=rev
        )

    def edge_val_ins(self, graph, nodeA, nodeB, idx, key, branch, rev, value):
        """Insert a record to indicate the value of a key on an edge as of
        ``(branch, rev)``

        """
        return self.conn.execute(
            self.sql['edge_val_ins'],
            graph=graph,
            orig=nodeA,
            dest=nodeB,
            idx=idx,
            key=key,
            branch=branch,
            rev=rev,
            value=value
        )

    def edge_val_upd(self, value, graph, nodeA, nodeB, idx, key, branch, rev):
        """Update a record previously inserted by ``edge_val_ins``"""
        return self.conn.execute(
            self.sql['edge_val_upd'],
            graph=graph,
            orig=nodeA,
            dest=nodeB,
            idx=idx,
            key=key,
            branch=branch,
            rev=rev
        )


if __name__ == '__main__':
    e = create_engine('sqlite:///:memory:')
    out = dict(
        (k, str(v)) for (k, v) in
        compile_sql(e.dialect, MetaData()).items()
    )

    print(dumps(out))

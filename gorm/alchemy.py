from sqlalchemy import (
    text,
    Table,
    Index,
    Column,
    CheckConstraint,
    ForeignKeyConstraint,
    Integer,
    Boolean,
    String,
    MetaData,
    ForeignKey
)
from sqlalchemy.sql import bindparam
from gorm.json import json_dump

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


node_val_compare_text = text(
    "SELECT before.key, before.value, after.value FROM "
    "(SELECT key, value, FROM node_val JOIN ("
    "SELECT graph, node, key, branch, MAX(rev) "
    "AS rev FROM node_val "
    "WHERE graph=:graphone "
    "AND node=:nodeone "
    "AND branch=:branchone "
    "AND rev<=:revone GROUP BY graph, node, key, branch) AS hirev1 "
    "ON node_val.graph=hirev1.graph "
    "AND node_val.node=hirev1.node "
    "AND node_val.key=hirev1.key "
    "AND node_val.branch=hirev1.branch "
    "AND node_val.rev=hirev1.rev"
    ") AS before FULL JOIN "
    "(SELECT key, value FROM node_val JOIN ("
    "SELECT graph, node, key, branch, "
    "MAX(rev) AS rev FROM node_val "
    "WHERE graph=:graphtwo "
    "AND node=:nodetwo "
    "AND branch=:branchtwo "
    "AND rev<=:revtwo GROUP BY graph, node, key, branch) AS hirev2 "
    "ON node_val.graph=hirev2.graph "
    "AND node_val.node=hirev2.node "
    "AND node_val.key=hirev2.key "
    "AND node_val.branch=hirev2.branch "
    "AND node_val.rev=hirev2.rev"
    ") AS after "
    "ON before.key=after.key "
    "WHERE before.value<>after.value"
    ";"
)


def node_val_compare(
        conn, graph1, node1, branch_before, rev_before,
        graph2, node2, branch_after, rev_after
):
    return conn.execute(
        node_val_compare_text,
        graphone=graph1,
        nodeone=node1,
        branchone=branch_before,
        revone=rev_before,
        graphtwo=graph2,
        nodetwo=node2,
        branchtwo=branch_after,
        revtwo=rev_after
    )

edge_val_compare_text = text(
    "SELECT before.key, before.value, after.value "
    "FROM (SELECT key, value FROM edge_val JOIN "
    "(SELECT graph, nodeA, nodeB, idx, key, branch, "
    "MAX(rev) AS rev "
    "FROM edge_val WHERE "
    "graph=:gone AND "
    "nodeA=:origone AND "
    "nodeB=:destone AND "
    "idx=:ione AND "
    "branch=:bone AND "
    "rev<=:rone "
    "GROUP BY graph, nodeA, nodeB, idx, key, branch) AS hirev1 "
    "ON edge_val.graph=hirev1.graph "
    "AND edge_val.nodeA=hirev1.nodeA "
    "AND edge_val.nodeB=hirev1.nodeB "
    "AND edge_val.idx=hirev1.idx "
    "AND edge_val.key=hirev1.key "
    "AND edge_val.branch=hirev1.branch "
    "AND edge_val.rev=hirev1.rev"
    ") AS before FULL JOIN "
    "(SELECT key, value FROM edge_val JOIN "
    "(SELECT graph, nodeA, nodeB, idx, key, branch, "
    "MAX(rev) AS rev "
    "FROM edge_val WHERE "
    "graph=:gtwo AND "
    "nodeA=:origtwo AND "
    "nodeB=:desttwo AND "
    "idx=:itwo AND "
    "branch=:btwo AND "
    "rev<=:rtwo "
    "GROUP BY graph, nodeA, nodeB, idx, key, branch) AS hirev2 "
    "ON edge_val.graph=hirev2.graph "
    "AND edge_val.nodeA=hirev2.nodeA "
    "AND edge_val.nodeB=hirev2.nodeB "
    "AND edge_val.idx=hirev2.idx "
    "AND edge_val.key=hirev2.key "
    "AND edge_val.branch=hirev2.branch "
    "AND edge_val.rev=hirev2.rev"
    ") AS after ON "
    "before.key=after.key "
    "WHERE before.value<>after.value"
    ";"
)


def edge_val_compare(
        conn, graph1, nodeA1, nodeB1, idx1, branch_before, rev_before,
        graph2, nodeA2, nodeB2, idx2, branch_after, rev_after
):
    return conn.execute(
        edge_val_compare_text,
        gone=graph1,
        origone=nodeA1,
        destone=nodeB1,
        ione=idx1,
        bone=branch_before,
        rone=rev_before,
        gtwo=graph2,
        origtwo=nodeA2,
        desttwo=nodeB2,
        itwo=idx2,
        btwo=branch_after,
        rtwo=rev_after
    )

graph_compare_text = text(
    "SELECT before.key, before.value, after.value "
    "FROM (SELECT key, value FROM graph_val JOIN "
    "(SELECT graph, key, branch, MAX(rev) AS rev "
    "FROM graph_val WHERE "
    "graph=:gone AND "
    "branch=:bone AND "
    "rev<=:rone GROUP BY graph, key, branch) AS hirev1 "
    "ON graph_val.graph=hirev1.graph "
    "AND graph_val.key=hirev1.key "
    "AND graph_val.branch=hirev1.branch "
    "AND graph_val.rev=hirev1.rev"
    ") AS before FULL JOIN "
    "(SELECT key, value FROM graph_val JOIN "
    "(SELECT graph, key, branch, MAX(rev) AS rev "
    "FROM graph_val WHERE "
    "graph=:gtwo AND "
    "branch=:btwo AND "
    "rev<=:rtwo GROUP BY graph, key, branch) AS hirev2 "
    "ON graph_val.graph=hirev2.graph "
    "AND graph_val.key=hirev2.key "
    "AND graph_val.branch=hirev2.branch "
    "AND graph_val.rev=hirev2.rev"
    ") AS after ON "
    "before.key=after.key "
    "WHERE before.value<>after.value"
    ";"
)


def graph_compare(
        conn, graph1, branch_before, rev_before,
        graph2, branch_after, rev_after
):
    return conn.execute(
        graph_compare_text,
        gone=graph1,
        bone=branch_before,
        rone=rev_before,
        gtwo=graph2,
        btwo=branch_after,
        rtwo=rev_after
    )


class Alchemist(object):
    def __init__(self, engine):
        self.engine = engine
        self.conn = self.engine.connect()
        self.ins_global_default_branch_compiled = table_global.insert().values(
            key=json_dump('branch'),
            value=json_dump('master')
        ).compile(dialect=self.engine.dialect)
        self.ins_global_default_rev_compiled = table_global.insert().values(
            key=json_dump('rev'),
            value=json_dump(0)
        ).compile(dialect=self.engine.dialect)
        self.ins_branches_defaults = table_branches.insert().values(
            branch='master',
            parent='master',
            parent_rev=0
        ).compile(dialect=self.engine.dialect)
        self.ctbranch_compiled = text(
            "SELECT COUNT(*) FROM branches WHERE branch=:branch;"
        ).compile(dialect=self.engine.dialect)
        self.ctgraph_compiled = text(
            "SELECT COUNT(*) FROM graphs WHERE graph=:graph;"
        ).compile(dialect=self.engine.dialect)
        self.allbranch_compiled = text(
            "SELECT branch, parent, parent_rev FROM branches;"
        ).compile(dialect=self.engine.dialect)
        self.global_key_compiled = text(
            "SELECT value FROM global WHERE key=:key;"
        ).compile(dialect=self.engine.dialect)
        self.new_graph_compiled = text(
            "INSERT INTO graphs (graph, type) VALUES (:graph, :typ);"
        ).compile(dialect=self.engine.dialect)
        self.new_branch_compiled = table_branches.insert().values(
            branch=bindparam('branch'),
            parent=bindparam('parent'),
            parent_rev=bindparam('parent_rev')
        ).compile(dialect=self.engine.dialect)
        self.del_edge_val_graph_compiled = table_edge_val.delete().where(
            table_edge_val.c.graph == bindparam('graph')
        ).compile(dialect=self.engine.dialect)
        self.del_node_val_graph_compiled = table_node_val.delete().where(
            table_node_val.c.graph == bindparam('graph')
        ).compile(dialect=self.engine.dialect)
        self.del_node_graph_compiled = table_nodes.delete().where(
            table_nodes.c.graph == bindparam('graph')
        ).compile(dialect=self.engine.dialect)
        self.del_graph_compiled = table_graphs.delete().where(
            table_graphs.c.graph == bindparam('graph')
        ).compile(dialect=self.engine.dialect)
        self.parrev_compiled = text(
            "SELECT parent_rev FROM branches WHERE branch=:branch;"
        ).compile(dialect=self.engine.dialect)
        self.parparrev_compiled = text(
            "SELECT parent, parent_rev FROM branches WHERE branch=:branch;"
        ).compile(dialect=self.engine.dialect)
        self.global_ins_compiled = table_global.insert().values(
            key=bindparam('k'),
            value=bindparam('v')
        ).compile(dialect=self.engine.dialect)
        self.global_set_compiled = table_global.update().values(
            value=bindparam('v')
        ).where(
            table_global.c.key == bindparam('k')
        )
        self.nodes_extant_compiled = text(
            "SELECT nodes.node "
            "FROM nodes JOIN ("
            "SELECT graph, node, branch, MAX(rev) AS rev FROM nodes "
            "WHERE graph=:g "
            "AND branch=:b "
            "AND rev<=:r "
            "GROUP BY graph, node, branch) AS hirev "
            "ON nodes.graph=hirev.graph "
            "AND nodes.node=hirev.node "
            "AND nodes.branch=hirev.branch "
            "AND nodes.rev=hirev.rev "
            "WHERE nodes.node IS NOT NULL "
            "AND nodes.extant;"
        ).compile(dialect=self.engine.dialect)
        self.node_exists_compiled = text(
            "SELECT nodes.extant FROM nodes JOIN ("
            "SELECT graph, node, branch, MAX(rev) AS rev FROM nodes "
            "WHERE graph=:g "
            "AND node=:n "
            "AND branch=:b "
            "AND rev<=:r "
            "GROUP BY graph, node, branch) AS hirev "
            "ON nodes.graph=hirev.graph "
            "AND nodes.node=hirev.node "
            "AND nodes.branch=hirev.branch "
            "AND nodes.rev=hirev.rev;"
        ).compile(dialect=self.engine.dialect)
        self.exist_node_ins_compiled = table_nodes.insert().values(
            graph=bindparam('g'),
            node=bindparam('n'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            extant=bindparam('x')
        ).compile(dialect=self.engine.dialect)
        self.exist_node_upd_compiled = text(
            "UPDATE nodes SET extant=:x "
            "WHERE graph=:g "
            "AND node=:n "
            "AND branch=:b "
            "AND rev=:r;"
        ).compile(dialect=self.engine.dialect)
        self.graph_val_keys_set_compiled = text(
            "SELECT graph_val.key "
            "FROM graph_val JOIN ("
            "SELECT graph, key, branch, MAX(rev) AS rev FROM graph_val "
            "WHERE graph=:g "
            "AND branch=:b "
            "AND rev<=:r "
            "GROUP BY graph, key, branch) AS hirev "
            "ON graph_val.graph=hirev.graph "
            "AND graph_val.key=hirev.key "
            "AND graph_val.branch=hirev.branch "
            "AND graph_val.rev=hirev.rev "
            "WHERE graph_val.value IS NOT NULL;"
        ).compile(dialect=self.engine.dialect)
        self.graph_val_key_set_compiled = text(
            "SELECT graph_val.value FROM graph_val JOIN "
            "(SELECT graph, key, branch, MAX(rev) AS rev "
            "FROM graph_val WHERE "
            "graph=:g AND "
            "key=:k AND "
            "branch=:b AND "
            "rev<=:r GROUP BY graph, key, branch) AS hirev ON "
            "graph_val.graph=hirev.graph AND "
            "graph_val.key=hirev.key AND "
            "graph_val.branch=hirev.branch AND "
            "graph_val.rev=hirev.rev;"
        ).compile(dialect=self.engine.dialect)
        self.graph_val_present_value_compiled = text(
            "SELECT value FROM graph_val JOIN ("
            "SELECT graph, key, branch, MAX(rev) AS rev "
            "FROM graph_val WHERE "
            "graph=:g AND "
            "key=:k AND "
            "branch=:b AND "
            "rev<=:r GROUP BY graph, key, branch) AS hirev "
            "ON graph_val.graph=hirev.graph "
            "AND graph_val.key=hirev.key "
            "AND graph_val.branch=hirev.branch "
            "AND graph_val.rev=hirev.rev;"
        ).compile(dialect=self.engine.dialect)
        self.graph_val_ins_compiled = table_graph_val.insert().values(
            graph=bindparam('g'),
            key=bindparam('k'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            value=bindparam('v')
        ).compile(dialect=self.engine.dialect)
        self.graph_val_upd_compiled = text(
            "UPDATE graph_val SET value=:v "
            "WHERE graph=:g "
            "AND key=:k "
            "AND branch=:b "
            "AND rev=:r;"
        ).compile(dialect=self.engine.dialect)
        self.node_val_keys_compiled = text(
            "SELECT node_val.key FROM node_val JOIN ("
            "SELECT graph, node, key, branch, MAX(rev) AS rev "
            "FROM node_val WHERE "
            "graph=:g AND "
            "node=:n AND "
            "branch=:b AND "
            "rev<=:r "
            "GROUP BY graph, node, key, branch) AS hirev ON "
            "node_val.graph=hirev.graph AND "
            "node_val.node=hirev.node AND "
            "node_val.key=hirev.key AND "
            "node_val.branch=hirev.branch AND "
            "node_val.rev=hirev.rev "
            "WHERE node_val.value IS NOT NULL;"
        ).compile(dialect=self.engine.dialect)
        self.node_val_get_compiled = text(
            "SELECT node_val.value FROM node_val JOIN ("
            "SELECT graph, node, key, branch, MAX(rev) AS rev "
            "FROM node_val WHERE "
            "graph=:g AND "
            "node=:n AND "
            "key=:k AND "
            "branch=:b AND "
            "rev<=:r "
            "GROUP BY graph, node, key, branch) AS hirev "
            "ON node_val.graph=hirev.graph "
            "AND node_val.node=hirev.node "
            "AND node_val.key=hirev.key "
            "AND node_val.branch=hirev.branch "
            "AND node_val.rev=hirev.rev "
            "WHERE node_val.value IS NOT NULL;"
        ).compile(dialect=self.engine.dialect)
        self.node_val_ins_compiled = table_node_val.insert().values(
            graph=bindparam('g'),
            node=bindparam('n'),
            key=bindparam('k'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            value=bindparam('v')
        ).compile(dialect=self.engine.dialect)
        self.node_val_upd_compiled = text(
            "UPDATE node_val SET value=:v WHERE "
            "graph=:g AND "
            "node=:n AND "
            "key=:k AND "
            "branch=:b AND "
            "rev=:r;"
        ).compile(dialect=self.engine.dialect)
        self.edge_extant_compiled = text(
            "SELECT edges.extant FROM edges JOIN ("
            "SELECT graph, nodeA, nodeB, idx, branch, "
            "MAX(rev) AS rev FROM edges "
            "WHERE graph=:g "
            "AND nodeA=:orig "
            "AND nodeB=:dest "
            "AND idx=:i "
            "AND branch=:b "
            "AND rev<=:r "
            "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev "
            "ON edges.graph=hirev.graph "
            "AND edges.nodeA=hirev.nodeA "
            "AND edges.nodeB=hirev.nodeB "
            "AND edges.idx=hirev.idx "
            "AND edges.branch=hirev.branch "
            "AND edges.rev=hirev.rev;"
        ).compile(dialect=self.engine.dialect)
        self.edge_exist_ins_compiled = table_edges.insert().values(
            graph=bindparam('g'),
            nodeA=bindparam('orig'),
            nodeB=bindparam('dest'),
            idx=bindparam('i'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            extant=bindparam('x')
        ).compile(dialect=self.engine.dialect)
        self.edge_exist_upd_compiled = text(
            "UPDATE edges SET extant=:x WHERE "
            "graph=:g AND "
            "nodeA=:orig AND "
            "nodeB=:dest AND "
            "idx=:i AND "
            "branch=:b AND "
            "rev=:r;"
        ).compile(dialect=self.engine.dialect)
        self.edge_val_keys_compiled = text(
            "SELECT edge_val.key FROM edge_val JOIN ("
            "SELECT graph, nodeA, nodeB, idx, key, branch, "
            "MAX(rev) AS rev "
            "FROM edge_val WHERE "
            "graph=:g AND "
            "nodeA=:orig AND "
            "nodeB=:dest AND "
            "idx=:i AND "
            "branch=:b AND "
            "rev<=:r GROUP BY graph, nodeA, nodeB, idx, key, branch) "
            "AS hirev "
            "ON edge_val.graph=hirev.graph "
            "AND edge_val.nodeA=hirev.nodeA "
            "AND edge_val.nodeB=hirev.nodeB "
            "AND edge_val.idx=hirev.idx "
            "AND edge_val.rev=hirev.rev "
            "WHERE edge_val.value IS NOT NULL;"
        ).compile(dialect=self.engine.dialect)
        self.edge_val_get_compiled = text(
            "SELECT edge_val.value FROM edge_val JOIN ("
            "SELECT graph, nodeA, nodeB, idx, key, branch, "
            "MAX(rev) AS rev "
            "FROM edge_val WHERE "
            "graph=:g AND "
            "nodeA=:orig AND "
            "nodeB=:dest AND "
            "idx=:i AND "
            "key=:k AND "
            "branch=:b AND "
            "rev<=:r "
            "GROUP BY graph, nodeA, nodeB, idx, key, branch) AS hirev "
            "ON edge_val.graph=hirev.graph "
            "AND edge_val.nodeA=hirev.nodeA "
            "AND edge_val.nodeB=hirev.nodeB "
            "AND edge_val.idx=hirev.idx "
            "AND edge_val.key=hirev.key "
            "AND edge_val.branch=hirev.branch "
            "AND edge_val.rev=hirev.rev "
            "WHERE edge_val.value IS NOT NULL;"
        ).compile(dialect=self.engine.dialect)
        self.edge_val_ins_compiled = table_edge_val.insert().values(
            graph=bindparam('g'),
            nodeA=bindparam('orig'),
            nodeB=bindparam('dest'),
            idx=bindparam('i'),
            key=bindparam('k'),
            branch=bindparam('b'),
            rev=bindparam('r'),
            value=bindparam('v')
        ).compile(dialect=self.engine.dialect)
        self.edge_val_upd_compiled = text(
            "UPDATE edge_val SET value=:v "
            "WHERE graph=:g "
            "AND nodeA=:orig "
            "AND nodeB=:dest "
            "AND idx=:i "
            "AND key=:k "
            "AND branch=:b "
            "AND rev=:r;"
        )
        self.edgeiter_compiled = text(
            "SELECT edges.nodeA, edges.extant FROM edges JOIN "
            "(SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
            "FROM edges WHERE "
            "graph=:g AND "
            "branch=:b AND "
            "rev<=:r GROUP BY "
            "graph, nodeA, nodeB, idx, branch) AS hirev ON "
            "edges.graph=hirev.graph AND "
            "edges.nodeA=hirev.nodeA AND "
            "edges.nodeB=hirev.nodeB AND "
            "edges.idx=hirev.idx AND "
            "edges.branch=hirev.branch AND "
            "edges.rev=hirev.rev;"
        ).compile(dialect=self.engine.dialect)
        self.nodeBiter_compiled = text(
            "SELECT edges.nodeB, edges.extant FROM edges JOIN ("
            "SELECT graph, nodeA, nodeB, branch, MAX(rev) AS rev "
            "FROM edges WHERE "
            "graph=:g AND "
            "nodeA=:orig AND "
            "branch=:b AND "
            "rev<=:r "
            "GROUP BY graph, nodeA, nodeB, branch) "
            "AS hirev ON "
            "edges.graph=hirev.graph AND "
            "edges.nodeA=hirev.nodeA AND "
            "edges.nodeB=hirev.nodeB AND "
            "edges.branch=hirev.branch AND "
            "edges.rev=hirev.rev;"
        ).compile(dialect=self.engine.dialect)
        self.nodeAiter_compiled = text(
            "SELECT edges.nodeA, edges.extant FROM edges JOIN ("
            "SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
            "FROM edges WHERE "
            "graph=:g AND "
            "nodeB=:dest AND "
            "branch=:b AND "
            "rev<=:r "
            "GROUP BY graph, nodeA, nodeB, idx, branch "
            ") AS hirev ON "
            "edges.graph=hirev.graph AND "
            "edges.nodeA=hirev.nodeA AND "
            "edges.nodeB=hirev.nodeB AND "
            "edges.idx=hirev.idx AND "
            "edges.branch=hirev.branch AND "
            "edges.rev=hirev.rev;"
        ).compile(dialect=self.engine.dialect)
        self.edge_exists_compiled = text(
            "SELECT edges.extant FROM edges JOIN "
            "(SELECT graph, nodeA, nodeB, idx, branch, "
            "MAX(rev) AS rev FROM edges WHERE "
            "graph=:g AND "
            "nodeA=:orig AND "
            "nodeB=:dest AND "
            "branch=:b AND "
            "rev<=:r "
            "GROUP BY graph, nodeA, nodeB, idx, branch"
            ") AS hirev ON "
            "edges.graph=hirev.graph AND "
            "edges.nodeA=hirev.nodeA AND "
            "edges.nodeB=hirev.nodeB AND "
            "edges.idx=hirev.idx AND "
            "edges.branch=hirev.branch AND "
            "edges.rev=hirev.rev;"
        ).compile(dialect=self.engine.dialect)
        self.multi_edges_iter_compiled = text(
            "SELECT edges.idx, edges.extant FROM edges JOIN ("
            "SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
            "FROM edges WHERE "
            "graph=:g AND "
            "nodeA=:orig AND "
            "nodeB=:dest AND "
            "branch=:b AND "
            "rev<=:r "
            "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev ON "
            "edges.graph=hirev.graph AND "
            "edges.nodeA=hirev.nodeA AND "
            "edges.nodeB=hirev.nodeB AND "
            "edges.idx=hirev.idx AND "
            "edges.branch=hirev.branch AND "
            "edges.rev=hirev.rev"
            ";"
        ).compile(dialect=self.engine.dialect)

    def ins_global_default_branch(self):
        return self.conn.execute(
            self.ins_global_default_branch_compiled
        )

    def ins_global_default_rev(self):
        return self.conn.execute(
            self.ins_global_default_rev_compiled
        )

    def ctbranch(self, branch):
        return self.conn.execute(
            self.ctbranch_compiled,
            branch=branch
        )

    def ctgraph(self, graph):
        return self.conn.execute(
            self.ctgraph_compiled,
            graph=graph
        )

    def allbranch(self):
        return self.conn.execute(
            self.allbranch_compiled
        )

    def global_key(self, key):
        return self.conn.execute(
            self.global_key_compiled,
            key=key
        )

    def new_graph(self, graph, typ):
        return self.conn.execute(
            self.new_graph_compiled,
            graph=graph,
            typ=typ
        )

    def new_branch(self, branch, parent, parent_rev):
        return self.conn.execute(
            self.new_branch_compiled,
            branch=branch,
            parent=parent,
            parent_rev=parent_rev
        )

    def del_edge_val_graph(self, graph):
        return self.conn.execute(
            self.del_edge_val_graph_compiled,
            graph=graph
        )

    def del_node_val_graph(self, graph):
        return self.conn.execute(
            self.del_node_val_graph_compiled,
            graph=graph
        )

    def del_node_graph(self, graph):
        return self.conn.execute(
            self.del_node_graph_compiled,
            graph=graph
        )

    def parrev(self, branch):
        return self.conn.execute(
            self.parrev_compiled,
            branch=branch
        )

    def parparrev(self, branch):
        return self.conn.execute(
            self.parparrev_compiled,
            branch=branch
        )

    def global_ins(self, key, value):
        return self.conn.execute(
            self.global_ins_compiled,
            k=key,
            v=value
        )

    def global_set(self, key, value):
        return self.conn.execute(
            self.global_set_compiled,
            k=key,
            v=value
        )

    def nodes_extant(self, graph, branch, rev):
        return self.conn.execute(
            self.nodes_extant_compiled,
            g=graph,
            b=branch,
            r=rev
        )

    def node_exists(self, graph, node, branch, rev):
        return self.conn.execute(
            self.node_exists_compiled,
            g=graph,
            n=node,
            b=branch,
            r=rev
        )

    def exist_node_ins(self, graph, node, branch, rev, extant):
        return self.conn.execute(
            self.exist_node_ins_compiled,
            g=graph,
            n=node,
            b=branch,
            r=rev,
            x=extant
        )

    def exist_node_upd(self, extant, graph, node, branch, rev):
        return self.conn.execute(
            self.exist_node_upd_compiled,
            x=extant,
            g=graph,
            n=node,
            b=branch,
            r=rev
        )

    def graph_val_keys_set(self, graph, branch, rev):
        return self.conn.execute(
            self.graph_val_keys_set_compiled,
            g=graph,
            b=branch,
            r=rev
        )

    def graph_val_key_set(self, graph, key, branch, rev):
        return self.conn.execute(
            self.graph_val_key_set_compiled,
            g=graph,
            k=key,
            b=branch,
            r=rev
        )

    def graph_val_present_value(self, graph, key, branch, rev):
        return self.conn.execute(
            self.graph_val_present_value_compiled,
            g=graph,
            k=key,
            b=branch,
            r=rev
        )

    def graph_val_ins(self, graph, key, branch, rev, value):
        return self.conn.execute(
            self.graph_val_ins_compiled,
            g=graph,
            k=key,
            b=branch,
            r=rev,
            v=value
        )

    def graph_val_upd(self, value, graph, key, branch, rev):
        return self.conn.execute(
            self.graph_val_upd_compiled,
            v=value,
            g=graph,
            k=key,
            b=branch,
            r=rev
        )

    def node_val_keys(self, graph, node, branch, rev):
        return self.conn.execute(
            self.node_val_keys_compiled,
            g=graph,
            n=node,
            b=branch,
            r=rev
        )

    def node_val_get(self, graph, node, key, branch, rev):
        return self.conn.execute(
            self.node_val_get_compiled,
            g=graph,
            n=node,
            k=key,
            b=branch,
            r=rev
        )

    def node_val_ins(self, graph, node, key, branch, rev, value):
        return self.conn.execute(
            self.node_val_ins_compiled,
            g=graph,
            n=node,
            k=key,
            b=branch,
            r=rev,
            v=value
        )

    def node_val_upd(self, value, graph, node, key, branch, rev):
        return self.conn.execute(
            self.node_val_upd_compiled,
            v=value,
            g=graph,
            n=node,
            k=key,
            b=branch,
            r=rev
        )

    def edge_extant(self, graph, nodeA, nodeB, idx, branch, rev):
        return self.conn.execute(
            self.edge_extant_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            b=branch,
            r=rev
        )

    def edge_exist_ins(self, graph, nodeA, nodeB, idx, branch, rev, extant):
        return self.conn.execute(
            self.edge_exist_ins_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            b=branch,
            r=rev,
            x=extant
        )

    def edge_exist_upd(self, extant, graph, nodeA, nodeB, idx, branch, rev):
        return self.conn.execute(
            self.edge_exist_upd_compiled,
            x=extant,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            b=branch,
            r=rev
        )

    def edge_val_keys(self, graph, nodeA, nodeB, idx, branch, rev):
        return self.conn.execute(
            self.edge_val_keys_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            b=branch,
            r=rev
        )

    def edge_val_get(self, graph, nodeA, nodeB, idx, key, branch, rev):
        return self.conn.execute(
            self.edge_val_get_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            k=key,
            b=branch,
            r=rev
        )

    def edge_val_ins(self, graph, nodeA, nodeB, idx, key, branch, rev, value):
        return self.conn.execute(
            self.edge_val_ins_compiled,
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
        return self.conn.execute(
            self.edge_val_upd_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            i=idx,
            k=key,
            b=branch,
            r=rev
        )

    def edgeiter(self, graph, branch, rev):
        return self.conn.execute(
            self.edgeiter_compiled,
            g=graph,
            b=branch,
            r=rev
        )

    def nodeBiter(self, graph, nodeA, branch, rev):
        return self.conn.execute(
            self.nodeBiter_compiled,
            g=graph,
            orig=nodeA,
            b=branch,
            r=rev
        )

    def nodeAiter(self, graph, nodeB, branch, rev):
        return self.conn.execute(
            self.nodeAiter_compiled,
            g=graph,
            dest=nodeB,
            b=branch,
            r=rev
        )

    def edge_exists(self, graph, nodeA, nodeB, branch, rev):
        return self.conn.execute(
            self.edge_exists_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            b=branch,
            r=rev
        )

    def multi_edges_iter(self, graph, nodeA, nodeB, branch, rev):
        return self.conn.execute(
            self.multi_edges_iter_compiled,
            g=graph,
            orig=nodeA,
            dest=nodeB,
            b=branch,
            r=rev
        )

from gorm.json import json_load


sqlite_strings = {
    'ctbranch':
    "SELECT COUNT(*) FROM branches WHERE branch=?;",
    'ctgraph':
    "SELECT COUNT(*) FROM graphs WHERE graph=?;",
    'allbranch':
    "SELECT branch, parent, parent_rev FROM branches;",
    'global_key':
    "SELECT value FROM global WHERE key=?;",
    'new_graph':
    "INSERT INTO graphs (graph, type) VALUES (?, ?);",
    'new_branch':
    "INSERT INTO branches (branch, parent, parent_rev) "
    "VALUES (?, ?, ?);",
    'del_edge_val_graph':
    "DELETE FROM edge_val WHERE graph=?;",
    'del_edge_graph':
    "DELETE FROM edges WHERE graph=?;",
    'del_node_val_graph':
    "DELETE FROM node_val WHERE graph=?;",
    'del_node_graph':
    "DELETE FROM nodes WHERE graph=?;",
    'del_graph':
    "DELETE FROM graphs WHERE graph=?;",
    'parrev':
    "SELECT parent_rev FROM branches WHERE branch=?;",
    'parparrev':
    "SELECT parent, parent_rev FROM branches WHERE branch=?;",
    'global_ins':
    "INSERT INTO global (key, value) VALUES (?, ?);",
    'global_set':
    "UPDATE global SET value=? WHERE key=?;",
    'decl_global':
    "CREATE TABLE global ("
    "key TEXT NOT NULL PRIMARY KEY, "
    "value TEXT)"
    ";",
    'decl_branches':
    "CREATE TABLE branches ("
    "branch TEXT NOT NULL DEFAULT 'master', "
    "parent TEXT NOT NULL DEFAULT 'master', "
    "parent_rev INTEGER NOT NULL DEFAULT 0, "
    "PRIMARY KEY(branch), "
    "FOREIGN KEY(parent) REFERENCES branches(branch)"
    ");",
    'decl_graphs':
    "CREATE TABLE graphs ("
    "graph TEXT NOT NULL, "
    "type TEXT NOT NULL DEFAULT 'Graph', "
    "PRIMARY KEY(graph), "
    "CHECK(type IN ('Graph', 'DiGraph', 'MultiGraph', 'MultiDiGraph'))"
    ");",
    'branches_defaults':
    "INSERT INTO branches DEFAULT VALUES;",
    'decl_graph_val':
    "CREATE TABLE graph_val ("
    "graph TEXT NOT NULL, "
    "key TEXT NOT NULL, "
    "branch TEXT NOT NULL DEFAULT 'master', "
    "rev INTEGER NOT NULL DEFAULT 0, "
    "value TEXT, "
    "PRIMARY KEY (graph, key, branch, rev), "
    "FOREIGN KEY(graph) REFERENCES graphs(graph), "
    "FOREIGN KEY(branch) REFERENCES branches(branch))"
    ";",
    'index_graph_val':
    "CREATE INDEX graph_val_idx ON graph_val(graph, key)"
    ";",
    'decl_nodes':
    "CREATE TABLE nodes ("
    "graph TEXT NOT NULL, "
    "node TEXT NOT NULL, "
    "branch TEXT NOT NULL DEFAULT 'master', "
    "rev INTEGER NOT NULL DEFAULT 0, "
    "extant BOOLEAN NOT NULL, "
    "PRIMARY KEY (graph, node, branch, rev), "
    "FOREIGN KEY(graph) REFERENCES graphs(graph), "
    "FOREIGN KEY(branch) REFERENCES branches(branch))"
    ";",
    'index_nodes':
    "CREATE INDEX nodes_idx ON nodes(graph, node)"
    ";",
    'decl_node_val':
    "CREATE TABLE node_val ("
    "graph TEXT NOT NULL, "
    "node TEXT NOT NULL, "
    "key TEXT NOT NULL, "
    "branch TEXT NOT NULL DEFAULT 'master', "
    "rev INTEGER NOT NULL DEFAULT 0, "
    "value TEXT, "
    "PRIMARY KEY(graph, node, key, branch, rev), "
    "FOREIGN KEY(graph, node) REFERENCES nodes(graph, node), "
    "FOREIGN KEY(branch) REFERENCES branches(branch))"
    ";",
    'index_node_val':
    "CREATE INDEX node_val_idx ON node_val(graph, node, key)"
    ";",
    'decl_edges':
    "CREATE TABLE edges ("
    "graph TEXT NOT NULL, "
    "nodeA TEXT NOT NULL, "
    "nodeB TEXT NOT NULL, "
    "idx INTEGER NOT NULL DEFAULT 0, "
    "branch TEXT NOT NULL DEFAULT 'master', "
    "rev INTEGER NOT NULL DEFAULT 0, "
    "extant BOOLEAN NOT NULL, "
    "PRIMARY KEY (graph, nodeA, nodeB, idx, branch, rev), "
    "FOREIGN KEY(graph, nodeA) REFERENCES nodes(graph, node), "
    "FOREIGN KEY(graph, nodeB) REFERENCES nodes(graph, node), "
    "FOREIGN KEY(branch) REFERENCES branches(branch))"
    ";",
    'index_edges':
    "CREATE INDEX edges_idx ON edges(graph, nodeA, nodeB, idx)"
    ";",
    'decl_edge_val':
    "CREATE TABLE edge_val ("
    "graph TEXT NOT NULL, "
    "nodeA TEXT NOT NULL, "
    "nodeB TEXT NOT NULL, "
    "idx INTEGER NOT NULL DEFAULT 0, "
    "key TEXT, "
    "branch TEXT NOT NULL DEFAULT 'master', "
    "rev INTEGER NOT NULL DEFAULT 0, "
    "value TEXT, "
    "PRIMARY KEY(graph, nodeA, nodeB, idx, key, branch, rev), "
    "FOREIGN KEY(graph, nodeA, nodeB, idx) "
    "REFERENCES edges(graph, nodeA, nodeB, idx), "
    "FOREIGN KEY(branch) REFERENCES branches(branch))"
    ";",
    'index_edge_val':
    "CREATE INDEX edge_val_idx ON edge_val(graph, nodeA, nodeB, idx, key)"
    ";",
    'nodes_extant':
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
    'node_exists':
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
    'graph_val_keys_set':
    "SELECT graph_val.key "
    "FROM graph_val JOIN ("
    "SELECT graph, key, branch, MAX(rev) AS rev FROM graph_val "
    "WHERE graph=? "
    "AND branch=? "
    "AND rev<=? "
    "GROUP BY graph, key, branch) AS hirev "
    "ON graph_val.graph=hirev.graph "
    "AND graph_val.key=hirev.key "
    "AND graph_val.branch=hirev.branch "
    "AND graph_val.rev=hirev.rev "
    "WHERE graph_val.value IS NOT NULL;",
    'graph_val_key_set':
    "SELECT graph_val.value FROM graph_val JOIN "
    "(SELECT graph, key, branch, MAX(rev) AS rev "
    "FROM graph_val WHERE "
    "graph=? AND "
    "key=? AND "
    "branch=? AND "
    "rev<=? GROUP BY graph, key, branch) AS hirev ON "
    "graph_val.graph=hirev.graph AND "
    "graph_val.key=hirev.key AND "
    "graph_val.branch=hirev.branch AND "
    "graph_val.rev=hirev.rev;",
    'graph_val_present_value':
    "SELECT value FROM graph_val JOIN ("
    "SELECT graph, key, branch, MAX(rev) AS rev "
    "FROM graph_val WHERE "
    "graph=? AND "
    "key=? AND "
    "branch=? AND "
    "rev<=? GROUP BY graph, key, branch) AS hirev "
    "ON graph_val.graph=hirev.graph "
    "AND graph_val.key=hirev.key "
    "AND graph_val.branch=hirev.branch "
    "AND graph_val.rev=hirev.rev;",
    'graph_val_ins':
    "INSERT INTO graph_val ("
    "graph, "
    "key, "
    "branch, "
    "rev, "
    "value) VALUES (?, ?, ?, ?, ?);",
    'graph_val_upd':
    "UPDATE graph_val SET value=? "
    "WHERE graph=? "
    "AND key=? "
    "AND branch=? "
    "AND rev=?;",
    'exist_node_ins':
    "INSERT INTO nodes ("
    "graph, "
    "node, "
    "branch, "
    "rev, "
    "extant) VALUES (?, ?, ?, ?, ?);",
    'exist_node_upd':
    "UPDATE nodes SET extant=? "
    "WHERE graph=? "
    "AND node=? "
    "AND branch=? "
    "AND rev=?;",
    'node_val_keys':
    "SELECT node_val.key FROM node_val JOIN ("
    "SELECT graph, node, key, branch, MAX(rev) AS rev "
    "FROM node_val WHERE "
    "graph=? AND "
    "node=? AND "
    "branch=? AND "
    "rev<=? "
    "GROUP BY graph, node, key, branch) AS hirev ON "
    "node_val.graph=hirev.graph AND "
    "node_val.node=hirev.node AND "
    "node_val.key=hirev.key AND "
    "node_val.branch=hirev.branch AND "
    "node_val.rev=hirev.rev "
    "WHERE node_val.value IS NOT NULL;",
    'node_val_get':
    "SELECT node_val.value FROM node_val JOIN ("
    "SELECT graph, node, key, branch, MAX(rev) AS rev "
    "FROM node_val WHERE "
    "graph=? AND "
    "node=? AND "
    "key=? AND "
    "branch=? AND "
    "rev<=? "
    "GROUP BY graph, node, key, branch) AS hirev "
    "ON node_val.graph=hirev.graph "
    "AND node_val.node=hirev.node "
    "AND node_val.key=hirev.key "
    "AND node_val.branch=hirev.branch "
    "AND node_val.rev=hirev.rev "
    "WHERE node_val.value IS NOT NULL;",
    'node_val_ins':
    "INSERT INTO node_val ("
    "graph, "
    "node, "
    "key, "
    "branch, "
    "rev, "
    "value) VALUES "
    "(?, ?, ?, ?, ?, ?);",
    'node_val_upd':
    "UPDATE node_val SET value=? WHERE "
    "graph=? AND "
    "node=? AND "
    "key=? AND "
    "branch=? AND "
    "rev=?;",
    'node_val_compare':
    "SELECT before.key, before.value, after.value FROM "
    "(SELECT key, value, FROM node_val JOIN ("
    "SELECT graph, node, key, branch, MAX(rev) "
    "AS rev FROM node_val "
    "WHERE graph=? "
    "AND node=? "
    "AND branch=? "
    "AND rev<=? GROUP BY graph, node, key, branch) AS hirev1 "
    "ON node_val.graph=hirev1.graph "
    "AND node_val.node=hirev1.node "
    "AND node_val.key=hirev1.key "
    "AND node_val.branch=hirev1.branch "
    "AND node_val.rev=hirev1.rev"
    ") AS before FULL JOIN "
    "(SELECT key, value FROM node_val JOIN ("
    "SELECT graph, node, key, branch, "
    "MAX(rev) AS rev FROM node_val "
    "WHERE graph=? "
    "AND node=? "
    "AND branch=? "
    "AND rev<=? GROUP BY graph, node, key, branch) AS hirev2 "
    "ON node_val.graph=hirev2.graph "
    "AND node_val.node=hirev2.node "
    "AND node_val.key=hirev2.key "
    "AND node_val.branch=hirev2.branch "
    "AND node_val.rev=hirev2.rev"
    ") AS after "
    "ON before.key=after.key "
    "WHERE before.value<>after.value"
    ";",
    'edge_extant':
    "SELECT edges.extant FROM edges JOIN ("
    "SELECT graph, nodeA, nodeB, idx, branch, "
    "MAX(rev) AS rev FROM edges "
    "WHERE graph=? "
    "AND nodeA=? "
    "AND nodeB=? "
    "AND idx=? "
    "AND branch=? "
    "AND rev<=? "
    "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev "
    "ON edges.graph=hirev.graph "
    "AND edges.nodeA=hirev.nodeA "
    "AND edges.nodeB=hirev.nodeB "
    "AND edges.idx=hirev.idx "
    "AND edges.branch=hirev.branch "
    "AND edges.rev=hirev.rev;",
    'edge_exist_ins':
    "INSERT INTO edges ("
    "graph, "
    "nodeA, "
    "nodeB, "
    "idx, "
    "branch, "
    "rev, "
    "extant) VALUES (?, ?, ?, ?, ?, ?, ?);",
    'edge_exist_upd':
    "UPDATE edges SET extant=? WHERE "
    "graph=? AND "
    "nodeA=? AND "
    "nodeB=? AND "
    "idx=? AND "
    "branch=? AND "
    "rev=?;",
    'edge_val_keys':
    "SELECT edge_val.key FROM edge_val JOIN ("
    "SELECT graph, nodeA, nodeB, idx, key, branch, "
    "MAX(rev) AS rev "
    "FROM edge_val WHERE "
    "graph=? AND "
    "nodeA=? AND "
    "nodeB=? AND "
    "idx=? AND "
    "branch=? AND "
    "rev<=? GROUP BY graph, nodeA, nodeB, idx, key, branch) "
    "AS hirev "
    "ON edge_val.graph=hirev.graph "
    "AND edge_val.nodeA=hirev.nodeA "
    "AND edge_val.nodeB=hirev.nodeB "
    "AND edge_val.idx=hirev.idx "
    "AND edge_val.rev=hirev.rev "
    "WHERE edge_val.value IS NOT NULL;",
    'edge_val_get':
    "SELECT edge_val.value FROM edge_val JOIN ("
    "SELECT graph, nodeA, nodeB, idx, key, branch, "
    "MAX(rev) AS rev "
    "FROM edge_val WHERE "
    "graph=? AND "
    "nodeA=? AND "
    "nodeB=? AND "
    "idx=? AND "
    "key=? AND "
    "branch=? AND "
    "rev<=? "
    "GROUP BY graph, nodeA, nodeB, idx, key, branch) AS hirev "
    "ON edge_val.graph=hirev.graph "
    "AND edge_val.nodeA=hirev.nodeA "
    "AND edge_val.nodeB=hirev.nodeB "
    "AND edge_val.idx=hirev.idx "
    "AND edge_val.key=hirev.key "
    "AND edge_val.branch=hirev.branch "
    "AND edge_val.rev=hirev.rev "
    "WHERE edge_val.value IS NOT NULL;",
    'edge_val_ins':
    "INSERT INTO edge_val ("
    "graph, "
    "nodeA, "
    "nodeB, "
    "idx, "
    "key, "
    "branch, "
    "rev, "
    "value) VALUES "
    "(?, ?, ?, ?, ?, ?, ?, ?);",
    'edge_val_upd':
    "UPDATE edge_val SET value=? "
    "WHERE graph=? "
    "AND nodeA=? "
    "AND nodeB=? "
    "AND idx=? "
    "AND key=? "
    "AND branch=? "
    "AND rev=?;",
    'edge_val_compare':
    "SELECT before.key, before.value, after.value "
    "FROM (SELECT key, value FROM edge_val JOIN "
    "(SELECT graph, nodeA, nodeB, idx, key, branch, "
    "MAX(rev) AS rev "
    "FROM edge_val WHERE "
    "graph=? AND "
    "nodeA=? AND "
    "nodeB=? AND "
    "idx=? AND "
    "branch=? AND "
    "rev<=? "
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
    "graph=? AND "
    "nodeA=? AND "
    "nodeB=? AND "
    "idx=? AND "
    "branch=? AND "
    "rev<=? "
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
    ";",
    'edgeiter':
    "SELECT edges.nodeA, edges.extant FROM edges JOIN "
    "(SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
    "FROM edges WHERE "
    "graph=? AND "
    "branch=? AND "
    "rev<=? GROUP BY "
    "graph, nodeA, nodeB, idx, branch) AS hirev ON "
    "edges.graph=hirev.graph AND "
    "edges.nodeA=hirev.nodeA AND "
    "edges.nodeB=hirev.nodeB AND "
    "edges.idx=hirev.idx AND "
    "edges.branch=hirev.branch AND "
    "edges.rev=hirev.rev;",
    'nodeBiter':
    "SELECT edges.nodeB, edges.extant FROM edges JOIN ("
    "SELECT graph, nodeA, nodeB, branch, MAX(rev) AS rev "
    "FROM edges WHERE "
    "graph=? AND "
    "nodeA=? AND "
    "branch=? AND "
    "rev<=? "
    "GROUP BY graph, nodeA, nodeB, branch) "
    "AS hirev ON "
    "edges.graph=hirev.graph AND "
    "edges.nodeA=hirev.nodeA AND "
    "edges.nodeB=hirev.nodeB AND "
    "edges.branch=hirev.branch AND "
    "edges.rev=hirev.rev;",
    'nodeAiter':
    "SELECT edges.nodeA, edges.extant FROM edges JOIN ("
    "SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
    "FROM edges WHERE "
    "graph=? AND "
    "nodeB=? AND "
    "branch=? AND "
    "rev<=? "
    "GROUP BY graph, nodeA, nodeB, idx, branch "
    ") AS hirev ON "
    "edges.graph=hirev.graph AND "
    "edges.nodeA=hirev.nodeA AND "
    "edges.nodeB=hirev.nodeB AND "
    "edges.idx=hirev.idx AND "
    "edges.branch=hirev.branch AND "
    "edges.rev=hirev.rev;",
    'edge_exists':
    "SELECT edges.extant FROM edges JOIN "
    "(SELECT graph, nodeA, nodeB, idx, branch, "
    "MAX(rev) AS rev FROM edges WHERE "
    "graph=? AND "
    "nodeA=? AND "
    "nodeB=? AND "
    "branch=? AND "
    "rev<=? "
    "GROUP BY graph, nodeA, nodeB, idx, branch"
    ") AS hirev ON "
    "edges.graph=hirev.graph AND "
    "edges.nodeA=hirev.nodeA AND "
    "edges.nodeB=hirev.nodeB AND "
    "edges.idx=hirev.idx AND "
    "edges.branch=hirev.branch AND "
    "edges.rev=hirev.rev;",
    'multi_edges_iter':
    "SELECT edges.idx, edges.extant FROM edges JOIN ("
    "SELECT graph, nodeA, nodeB, idx, branch, MAX(rev) AS rev "
    "FROM edges WHERE "
    "graph=? AND "
    "nodeA=? AND "
    "nodeB=? AND "
    "branch=? AND "
    "rev<=? "
    "GROUP BY graph, nodeA, nodeB, idx, branch) AS hirev ON "
    "edges.graph=hirev.graph AND "
    "edges.nodeA=hirev.nodeA AND "
    "edges.nodeB=hirev.nodeB AND "
    "edges.idx=hirev.idx AND "
    "edges.branch=hirev.branch AND "
    "edges.rev=hirev.rev"
    ";",
    'graph_compare':
    "SELECT before.key, before.value, after.value "
    "FROM (SELECT key, value FROM graph_val JOIN "
    "(SELECT graph, key, branch, MAX(rev) AS rev "
    "FROM graph_val WHERE "
    "graph=? AND "
    "branch=? AND "
    "rev<=? GROUP BY graph, key, branch) AS hirev1 "
    "ON graph_val.graph=hirev1.graph "
    "AND graph_val.key=hirev1.key "
    "AND graph_val.branch=hirev1.branch "
    "AND graph_val.rev=hirev1.rev"
    ") AS before FULL JOIN "
    "(SELECT key, value FROM graph_val JOIN "
    "(SELECT graph, key, branch, MAX(rev) AS rev "
    "FROM graph_val WHERE "
    "graph=? AND "
    "branch=? AND "
    "rev<=? GROUP BY graph, key, branch) AS hirev2 "
    "ON graph_val.graph=hirev2.graph "
    "AND graph_val.key=hirev2.key "
    "AND graph_val.branch=hirev2.branch "
    "AND graph_val.rev=hirev2.rev"
    ") AS after ON "
    "before.key=after.key "
    "WHERE before.value<>after.value"
    ";"
}

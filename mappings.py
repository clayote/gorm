from collections import MutableMapping
from record import (
    NodeValRecord,
    EdgeValRecord,
    GraphValRecord
)


class GraphMapping(MutableMapping):
    def __init__(self, gorm, graph):
        self.gorm = gorm
        self.graph = graph

    def __getitem__(self, key):
        return self.gorm.current_value('graph_val', self.graph, key)

    def __setitem__(self, key, value):
        branch = self.gorm.cache['branch']
        rev = self.gorm.cache['rev']
        self.gorm.set_record(GraphValRecord(
            graph=self.graph,
            node=self.node,
            key=key,
            branch=branch,
            rev=rev,
            value=value
        ))

    def __delitem__(self, key):
        self[key] = None

    def __iter__(self):
        for key in self.gorm.cache['graph_val'][self.graph]:
            try:
                self.__getitem__(key)
                yield key
            except KeyError:
                continue

    def __len__(self):
        i = 0
        for k in iter(self):
            i += 1
        return i


class NodeMapping(MutableMapping):
    def __init__(self, gorm, graph, node):
        self.gorm = gorm
        self.graph = graph
        self.node = node

    def __getitem__(self, key):
        return self.gorm.current_value('node_val', self.graph, self.node, key)

    def __setitem__(self, key, value):
        branch = self.gorm.cache['branch']
        rev = self.gorm.cache['rev']
        recs = [
            NodeValRecord(
                graph=self.graph,
                node=self.node,
                key=key,
                branch=branch,
                rev=rev,
                value=value
            ),
            NodeValRecord(
                graph=self.graph,
                node=self.node,
                key=None,
                branch=branch,
                rev=rev,
                value=True
            )
        ]
        for rec in recs:
            self.gorm.set_record(rec)

    def __delitem__(self, key):
        self[key][None] = False

    def __iter__(self):
        skel = self.gorm.cache['node_val'][self.graph][self.node]
        for key in skel:
            try:
                self.__getitem__(key)
                yield key
            except KeyError:
                continue

    def __len__(self):
        i = 0
        for k in iter(self):
            i += 1
        return i


class EdgeMapping(MutableMapping):
    def __init__(self, gorm, graph, nodeA, nodeB, idx):
        self.gorm = gorm
        self.graph = graph
        self.nodeA = nodeA
        self.nodeB = nodeB
        self.idx = idx

    def __getitem__(self, key):
        return self.gorm.current_value('edge_val', self.graph, self.nodeA, self.nodeB, self.idx, keey)

    def __setitem__(self, key, value):
        branch = self.gorm.cache['branch']
        rev = self.gorm.cache['rev']
        recs = [
            NodeValRecord(
                graph=self.graph,
                node=self.nodeA,
                key=None,
                branch=branch,
                rev=rev,
                value=True
            ),
            NodeValRecord(
                graph=self.graph,
                node=self.nodeB,
                key=None,
                branch=branch,
                rev=rev,
                value=True
            ),
            EdgeValRecord(
                graph=self.graph,
                nodeA=self.nodeA,
                nodeB=self.nodeB,
                idx=self.idx,
                key=None,
                branch=branch,
                rev=rev,
                value=True
            ),
            EdgeValRecord(
                graph=self.graph,
                nodeA=self.nodeA,
                nodeB=self.nodeB,
                idx=self.idx,
                key=key,
                branch=branch,
                rev=rev,
                value=value
            )
        ]
        for rec in recs:
            self.gorm.set_record(rec)

    def __delitem__(self, key):
        self[key][None] = False

    def __iter__(self):
        i = 0
        for k in iter(self):
            i += 1
        return i


class GraphNodeMapping(MutableMapping):
    def __init__(self, gorm, graph):
        self.gorm = gorm
        self.graph = graph

    def __getitem__(self, node):
        if self.gorm.node_exists(self.graph, node):
            return NodeMapping(self.gorm, self.graph, node)
        else:
            raise KeyError("No such node")

    def __setitem__(self, node, value):
        nodemap = NodeMapping(self.gorm, self.graph, node)
        # indicate that the node exists
        nodemap[None] = True
        # presently I only support dictionary nodes
        for (k, v) in value.iteritems():
            nodemap[k] = v

    def __delitem__(self, node):
        self[node][None] = False

    def __iter__(self):
        skel = self.gorm.cache['node_val'][self.graph]
        for node in skel:
            if self.gorm.node_exists(self.graph, node):
                yield node


class GraphEdgeFinalMapping(MutableMapping):
    def __init__(self, gorm, graph, nodeA, nodeB):
        self.gorm = gorm
        self.graph = graph
        self.nodeA = nodeA
        self.nodeB = nodeB

    def __getitem__(self, key):
        if self.gorm.edge_exists(self.graph, self.nodeA, self.nodeB, key):
            return EdgeMapping(self.graph, self.nodeA, self.nodeB, key)
        else:
            return KeyError("No such edge")

    def __setitem__(self, key, value):
        edgemap = EdgeMapping(self.gorm, self.graph, self.nodeA, self.nodeB, key)
        edgemap[None] = True  # "I exist"
        for (k, v) in value.iteritems():
            edgemap[k] = v

    def __delitem__(self, key):
        self[key][None] = False

    def __iter__(self):
        for key in self.gorm.cache['edge_val'][self.graph][self.nodeA][self.nodeB]:
            if self.gorm.edge_exists(self.graph, self.nodeA, self.nodeB, key):
                yield key

    def __len__(self):
        i = 0
        for k in iter(self):
            i += 1
        return i


class GraphEdgeSuccessorMapping(MutableMapping):
    def __init__(self, gorm, graph, nodeA):
        self.gorm = gorm
        self.graph = graph
        self.nodeA = nodeA

    def __getitem__(self, nodeB):
        if self.gorm.edge_exists(self.graph, self.nodeA, nodeB):
            if self.graph.type in ("MultiGraph", "MultiDiGraph"):
                return GraphEdgeFinalMapping(self.gorm, self.graph, self.nodeA, nodeB)
            else:
                return EdgeMapping(self.gorm, self.graph, self.nodeA, nodeB, 0)
        else:
            raise KeyError("No such edge")

    def __setitem__(self, key, value):
        mapping = self[key]
        mapping[None] = True
        for (k, v) in value.iteritems():
            mapping[k] = v

    def __delitem__(self, idx):
        self[idx][None] = False

    def __iter__(self):
        skel = self.gorm.cache['edge_val'][self.graph][self.nodeA]
        for nodeB in skel:
            if self.gorm.edge_exists(self.graph, self.nodeA, nodeB):
                yield nodeB

    def __len__(self):
        i = 0
        for nodeB in iter(self):
            i += 1
        return i


class GraphEdgePredecessorMapping(MutableMapping):
    def __init__(self, gorm, graph):
        self.gorm = gorm
        self.graph = graph

    def __getitem__(self, key):
        if self.gorm.node_exists(self.graph, key):
            return GraphEdgeSuccessorMapping(self.gorm, self.graph, key)
        else:
            raise KeyError("No such node")

    def __setitem__(self, key, value):
        mapping = self[key]
        for (k, v) in value.iteritems():
            mapping[k] = v

    def __iter__(self):
        for node in self.gorm.cache['node_var']:

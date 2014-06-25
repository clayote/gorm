import networkx
from collections import MutableMapping
from record import (
    NodeRecord,
    NodeValRecord,
    EdgeRecord,
    EdgeValRecord,
    GraphValRecord
)


def value_during(branch_tree, skel, branch, rev):
    """Get the newest value at or before a particular revision in a
    particular branch.

    """
    if branch in skel:
        if rev in skel[branch]:
            return skel[branch][rev]
        return skel[branch][max(
            r for r in skel[branch]
            if r < rev
        )]
    elif branch == "master":
        raise KeyError("Could not find value")
    else:
        return value_during(
            branch_tree,
            skel,
            branch_tree[branch].parent,
            branch_tree[branch].rev
        )


class FinalMapping(MutableMapping):
    """Mapping for attributes of nodes, edges, and graphs."""
    def __getitem__(self, key):
        if key not in self.d:
            raise KeyError("Key never set")
        branch = self.gorm.cache['branch']
        rev = self.gorm.cache['rev']
        branch_tree = self.gorm.cache['branches']
        return value_during(
            branch_tree,
            self.__d,
            branch,
            rev
        )

    def __delitem__(self, k):
        self[k] = None

    def __iter__(self):
        for k in self.d:
            if self[k] is not None:
                yield k

    def __len__(self):
        n = 0
        for k in self:
            n += 1
        return n

    def clear(self):
        for k in self:
            del self[k]


class GraphMapping(FinalMapping):
    def __init__(self, graph):
        self.graph = graph
        self.gorm = graph.gorm
        self.__d = {}

    def __setitem__(self, key, value):
        branch = self.gorm.cache['branch']
        rev = self.gorm.cache['rev']
        self.gorm.writerec(GraphValRecord(
            graph=self.graph,
            node=self.node,
            key=key,
            branch=branch,
            rev=rev,
            value=value
        ))


class GraphNodeMapping(MutableMapping):
    class Node(FinalMapping):
        def __init__(self, graph, node):
            self.graph = graph
            self.gorm = graph.gorm
            self.node = node
            self.__d = {}
            self.graph.gorm.cache['node_val'][graph.name][node] = self
            # set a record indicating that this node exists
            self.gorm.writerec(NodeRecord(
                graph=graph.name,
                node=node,
                branch=self.gorm.cache['branch'],
                rev=self.gorm.cache['rev'],
                exists=True
            ))

        def __setitem__(self, key, value):
            self.gorm.writerec(NodeValRecord(
                graph=self.graph.name,
                node=self.node,
                key=key,
                branch=self.gorm.cache['branch'],
                rev=self.gorm.cache['rev'],
                value=value
            ))

    def __init__(self, graph):
        self.graph = graph
        self.gorm = graph.gorm
        self.__d = {}
        self.gorm.cache['node_val'][self.graph.name] = self

    def __getitem__(self, node):
        return self.__d[node]

    def __setitem__(self, node, dikt):
        if node in self.__d:
            self.__d[node].clear()
        else:
            self.__d[node] = GraphNodeMapping.Node(self.graph, node)
        self.__d[node].update(dikt)

    def __delitem__(self, node):
        if node not in self.__d:
            raise KeyError("No such node")
        self.gorm.writerec(NodeRecord(
            graph=self.graph.name,
            node=node,
            branch=self.gorm.cache['branch'],
            rev=self.gorm.cache['rev'],
            exists=False
        ))
        del self.__d[node]

    def __iter__(self):
        return iter(self.__d)

    def __len__(self):
        return len(self.__d)


class GraphEdgeMapping(MutableMapping):
    """Provides an adjacency mapping and possibly a predecessor mapping
    for a graph.

    """
    class Edge(FinalMapping):
        def __init__(self, graph, nodeA, nodeB, idx=0):
            """Initialize private dictionary and insert myself into the ORM's
            cache

            """
            self.graph = graph
            self.gorm = graph.gorm
            self.nodeA = nodeA
            self.nodeB = nodeB
            self.idx = idx
            self.__d = {}
            ptr = self.gorm.cache
            for key in ('edge_val', graph.name, nodeA, nodeB):
                if key not in ptr:
                    ptr[key] = {}
                ptr = ptr[key]
            ptr[idx] = self

        def __setitem__(self, key, value):
            self.gorm.writerec(EdgeValRecord(
                graph=self.graph.name,
                nodeA=self.nodeA,
                nodeB=self.nodeB,
                idx=self.idx,
                key=key,
                branch=self.gorm.cache['branch'],
                rev=self.gorm.cache['rev'],
                value=value,
                type=self.gorm.type2str[type(value)]
            ))

    class Node2Node(MutableMapping):
        """Mapping for when the predecessor and successor nodes are specified
        but the edge index is not.
        
        For use only in multigraphs.
        
        """
        def __init__(self, graph, nodeA, nodeB):
            """Start a private dictionary to hold Edge objects"""
            self.graph = graph
            self.gorm = graph.gorm
            self.nodeA = nodeA
            self.nodeB = nodeB
            self.__d = {}

        def __iter__(self):
            """Iterate over edge indices"""
            for k in self.d.iterkeys():
                yield k

        def __len__(self):
            """Return number of edges"""
            return len(self.__d)

        def __getitem__(self, idx):
            """Return the edge with the requested index"""
            return self.__d[idx]

        def __setitem__(self, k, v):
            """Interpret the value as a dictionary of Edge attributes and assign
            them to a new Edge

            """
            e = GraphEdgeMapping.Edge(self.graph, self.nodeA, self.nodeB, k)
            e.clear()
            e.update(v)
            self.__d[k] = e

        def __delitem__(self, k):
            if k not in self.__d:
                raise KeyError("No such edge")
            self.gorm.writerec(EdgeRecord(
                graph=self.graph,
                nodeA=self.nodeA,
                nodeB=self.nodeB,
                idx=k,
                branch=self.gorm.cache['branch'],
                tick=self.gorm.cache['tick'],
                exists=False
            ))
            del self.__d[k]

    class Preceded(MutableMapping):
        """Mapping for when only the first node of an edge has been specified.

        Might map to an Edge or to a Node2Node depending on what kind
        of graph this is.

        """
        def __init__(self, graph, nodeA):
            self.graph = graph
            self.gorm = graph.gorm
            self.nodeA = nodeA
            self.__d = {}

        def __iter__(self):
            return iter(self.__d)

        def __len__(self):
            return len(self.__d)

        def __getitem__(self, nodeB):
            return self.__d[nodeB]

        def __setitem__(self, nodeB, edge_d):
            """Interpret edge_d as a dictionary of dictionaries of edge
            attributes, itself keyed by the edges' indices. Set a new
            Edge for each, and wrap them all in a new Node2Node, to be
            kept herein with key nodeB.

            """
            n2n = GraphEdgeMapping.Node2Node(self.graph, self.nodeA, nodeB)
            # there might be some such edges in the database
            # already. They're getting overwritten now
            n2n.clear()

class Graph(networkx.Graph):
    def __init__(self, gorm, name, data=None, **attr):
        """A version of the networkx.Graph class that stores its state in a
        database.

        For the most part, works just like networkx.Graph, but you
        can't change its name after creation, and you can't assign
        None as the value of any key--or rather, doing so is
        considered eqivalent to deleting the key altogether.

        """
        self.name = name
        self.gorm = gorm
        self.graph = GraphMapping(self)
        self.node = GraphNodeMapping(self)
        self.adj = GraphSuccessorMapping(self)
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj

    def clear(self):
        """Remove all nodes and edges from the graph.

        Unlike the regular networkx implementation, this does *not*
        remove the graph's name. But all the other graph, node, and
        edge attributes go away.

        """
        self.adj.clear()
        self.node.clear()
        self.graph.clear()


class DiGraph(networkx.DiGraph):
    def __init__(self, gorm, name, data=None, **attr):

        """A version of the networkx.DiGraph class that stores its state in a
        database.

        For the most part, works just like networkx.DiGraph, but you
        can't change its name after creation, and you can't assign
        None as the value of any key--or rather, doing so is
        considered eqivalent to deleting the key altogether.

        """
        self.graph = GraphMapping(gorm, name)
        self.node = GraphNodeMapping(gorm, name)
        self.adj = GraphSuccessorMapping(gorm, name)
        self.pred = DiGraphPredecessorMapping(gorm, name)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj


class MultiGraph(networkx.MultiGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self.graph = GraphMapping(gorm, name)
        self.node = GraphNodeMapping(gorm, name)
        self.adj = MultiGraphSuccessorMapping(gorm, name)
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj


class MultiDiGraph(networkx.MultiDiGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self.graph = GraphMapping(gorm, name)
        self.node = GraphNodeMapping(gorm, name)
        self.adj = MultiGraphSuccessorMapping(gorm, name)
        self.pred = MultiGraphPredecessorMapping(gorm, name)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj

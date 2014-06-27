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
        """Get the most recent value of the key"""
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
        """Set the key to None, which will not be considered as "set" for the
        purposes of eg. ``__iter__``

        """
        self[k] = None

    def __iter__(self):
        """Yield the keys in my private dict, but only if they're not set to
        None, as that is taken to indicate unsetness.

        """
        for k in self.__d:
            if self[k] is not None:
                yield k

    def __len__(self):
        """Count all the keys not set to None"""
        n = 0
        for k in self:
            n += 1
        return n


class GraphMapping(FinalMapping):
    """Mapping for graph attributes"""
    def __init__(self, graph):
        """Initialize private dict and store pointers to the graph and ORM"""
        self.graph = graph
        self.gorm = graph.gorm
        self.__d = {}

    def __setitem__(self, key, value):
        """Set key=value at the present branch and revision"""
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

    def clear(self):
        """Set everything to None"""
        for k in self:
            del self[k]


class NodeOrEdgeMapping(FinalMapping):
    @property
    def exists(self):
        branch = self.gorm.cache['branch']
        rev = self.gorm.cache['rev']
        if branch not in self.__existence:
            return False
        try:
            return value_during(
                self.gorm.cache['branches'],
                self.__existence,
                branch,
                rev
            )
        except KeyError:
            return False

    @exists.setter
    def set_existence(self, val):
        if not isinstance(val, bool):
            raise TypeError("Existence is boolean")
        branch = self.gorm.cache['branch']
        rev = self.gorm.cache['rev']
        if branch not in self.__existence:
            self.__existence[branch] = {}
        self.__existence[branch][rev] = val
        self.gorm.writerec(self.__existence_cls(
            graph=self.graph.name,
            node=self.node,
            branch=branch,
            rev=rev,
            exists=val
        ))

    def clear(self):
        """Set everything to None, and stop existing"""
        for k in self:
            del self[k]
        self.exists = False


class GraphNodeMapping(MutableMapping):
    """Mapping for nodes in a graph"""
    class Node(NodeOrEdgeMapping):
        """Mapping for node attributes"""
        __existence_cls = NodeRecord

        def __init__(self, graph, node, existence=None, data=None):
            self.graph = graph
            self.gorm = graph.gorm
            self.node = node
            if existence is None:
                self.__existence = {}
            else:
                self.__existence = existence
            if data is None:
                self.__d = {}
            else:
                self.__d = data

        def __setitem__(self, key, value):
            """Set key=value at the present branch and revision."""
            branch = self.gorm.cache['branch']
            rev = self.gorm.cache['rev']
            if branch not in self.__d[key]:
                self.__d[key][branch] = {}
            self.__d[key][branch][rev] = value
            self.gorm.writerec(NodeValRecord(
                graph=self.graph.name,
                node=self.node,
                key=key,
                branch=self.gorm.cache['branch'],
                rev=self.gorm.cache['rev'],
                value=value,
                type=self.gorm.type2str[type(value)]
            ))

    def __init__(self, graph):
        """Initialize private dictionary"""
        self.graph = graph
        self.gorm = graph.gorm
        self.__d = {}

    def __getitem__(self, node):
        """Delegate to private dictionary"""
        return self.__d[node]

    def __setitem__(self, node, dikt):
        """Only accept dict-like values for assignment. These are taken to be
        dicts of node attributes, and so, a new GraphNodeMapping.Node
        is made with them, perhaps clearing out the one already there.

        """
        if node in self.__d:
            self.__d[node].clear()
        else:
            self.__d[node] = self.Node(self.graph, node)
        self.__d[node].exists = True
        self.__d[node].update(dikt)

    def __delitem__(self, node):
        """Indicate that the given node no longer exists"""
        if node not in self.__d:
            raise KeyError("No such node")
        self.__d[node].exists = False

    def __iter__(self):
        for (k, v) in self.__d.iteritems():
            if v.exists:
                yield k

    def __len__(self):
        n = 0
        for k in iter(self):
            n += 1
        return n


class GraphEdgeMapping(MutableMapping):
    """Provides an adjacency mapping and possibly a predecessor mapping
    for a graph.

    """
    class Edge(NodeOrEdgeMapping):
        __existence_cls = EdgeRecord

        def __init__(self, graph, nodeA, nodeB, idx=0, existence=None, data=None):
            self.graph = graph
            self.gorm = graph.gorm
            self.nodeA = nodeA
            self.nodeB = nodeB
            self.idx = idx
            if existence is None:
                self.__existence = {}
            else:
                self.__existence = existence
            if data is None:
                self.__d = {}
            else:
                self.__d = data

        def __setitem__(self, key, value):
            """Set a database record to say that key=value at the present branch
            and revision

            """
            branch = self.gorm.cache['branch']
            rev = self.gorm.cache['rev']
            if branch not in self.__d[key]:
                self.__d[key][branch] = {}
            self.__d[key][branch][rev] = value
            self.gorm.writerec(EdgeValRecord(
                graph=self.graph.name,
                nodeA=self.nodeA,
                nodeB=self.nodeB,
                idx=self.idx,
                key=key,
                branch=branch,
                rev=rev,
                value=value,
                type=self.gorm.type2str[type(value)]
            ))

    class EdgeBackward(Edge):
        """Edge with the nodeA and nodeB reversed, for predecessor maps."""
        def __init__(self, graph, nodeB, nodeA, idx=0, existence=None, data=None):
            return super(GraphEdgeMapping.Edge, self).__init__(
                self,
                graph,
                nodeA,
                nodeB,
                idx,
                existence,
                data
            )

    class Cessors(MutableMapping):
        """Mapping for when one node of the edge has been specified.

        Might map to predecessors or successors depending on what you put in it.

        Might map to edges directly or to another mapping, depending on its __sub_cls.

        """
        def __init__(self, graph, nodeA, sub_cls):
            self.graph = graph
            self.gorm = graph.gorm
            self.nodeA = nodeA
            self.__d = {}

        def __iter__(self):
            """Delegate to private dict"""
            return iter(self.__d)

        def __len__(self):
            """Delegate to private dict"""
            return len(self.__d)

        def __getitem__(self, nodeB):
            """Delegate to private dict"""
            return self.__d[nodeB]

        def __setitem__(self, nodeB, value):
            if nodeB in self.__d:
                v = self.__d[nodeB]
            else:
                v = self.__sub_cls(self.graph, self.nodeA)
            v.clear()
            v.exists = True
            v.update(value)

        def __delitem__(self, nodeB):
            self.__d[nodeB].clear()
            del self.__d[nodeB]

        def clear(self):
            for v in self.__d.itervalues():
                v.clear()
            self.__d = {}

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
            for (idx, edge) in self.__d.iteritems():
                if edge.exists:
                    yield idx

        def __len__(self):
            """Return number of edges that presently exist"""
            n = 0
            for edge in self.__d.itervalues():
                if edge.exists:
                    n += 1
            return n

        def __getitem__(self, idx):
            """Return the edge with the requested index, if it presently exists"""
            if idx not in self.__d:
                raise KeyError("There never was such an edge")
            e = self.__d[idx]
            if not e.exists:
                raise KeyError("No such edge at present")
            return e

        def __setitem__(self, k, v):
            """Interpret the value as a dictionary of Edge attributes and assign
            them to a new Edge

            """
            if k in self.__d:
                e = self.__d[k]
            else:
                e = self.__sub_cls(self.graph, self.nodeA, self.nodeB, k)
            e.clear()
            e.exists = True
            e.update(v)
            self.__d[k] = e

        def __delitem__(self, k):
            if k not in self.__d:
                raise KeyError("No such edge")
            if not self.__d[k].exists:
                raise KeyError("Edge already deleted")
            self.__d[k].exists = False

        def clear(self):
            for e in self.__d.itervalues():
                e.clear()
            self.__d = {}

    def __init__(self, graph):
        """Start a private dict to hold Predecessors or Successors instances"""
        self.graph = graph
        self.gorm = graph.gorm
        self.__d = {}

    def __iter__(self):
        """Delegate to private dict"""
        return iter(self.__d)

    def __len__(self):
        """Delegate to private dict"""
        return len(self.__d)

    def __getitem__(self, k):
        """Delegate to private dict"""
        return self.__d[k]

    def __setitem__(self, key, value):
        """Interpret the value as a dictionary of dictionaries of either Edge
        instances or dictionaries of Edge instances, depending on if
        I'm a multigraph.

        """
        if key in self.__d:
            v = self.__d[key]
        else:
            v = self.__sub_cls(self.graph, key)
        v.clear()
        v.update(value)


class GraphSuccessorsMapping(GraphEdgeMapping):
    class Successors(GraphEdgeMapping.Cessors):
        __sub_cls = GraphEdgeMapping.Edge

    __sub_cls = Successors


class DiGraphPredecessorsMapping(GraphEdgeMapping):
    class Predecessors(GraphEdgeMapping.Cessors):
        __sub_cls = GraphEdgeMapping.EdgeBackward

    __sub_cls = Predecessors


class MultiGraphSuccessorsMapping(GraphEdgeMapping):
    def __new__(cls, *args, **kwargs):
        class NodeA2NodeB(cls.Node2Node):
            __sub_cls = cls.Edge

        class Successors(cls.Cessors):
            __sub_cls = NodeA2NodeB

        r = super(MultiGraphSuccessorsMapping, cls).__new__(cls, *args, **kwargs)
        r.__sub_cls = Successors
        return r


class MultiDiGraphPredecessorsMapping(GraphEdgeMapping):
    def __new__(cls, *args, **kwargs):
        class NodeB2NodeA(cls.Node2Node):
            __sub_cls = cls.EdgeBackward

        class Predecessors(cls.Cessors):
            __sub_cls = NodeB2NodeA

        r = super(MultiDiGraphPredecessorsMapping, cls).__new__(cls, *args, **kwargs)
        r.__sub_cls = Predecessors
        return r


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
        self.adj = GraphSuccessorsMapping(self)
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
        self.adj = GraphSuccessorsMapping(gorm, name)
        self.pred = DiGraphPredecessorsMapping(gorm, name)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj


class MultiGraph(networkx.MultiGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self.graph = GraphMapping(gorm, name)
        self.node = GraphNodeMapping(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj


class MultiDiGraph(networkx.MultiDiGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self.graph = GraphMapping(gorm, name)
        self.node = GraphNodeMapping(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        self.pred = MultiDiGraphPredecessorsMapping(gorm, name)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)
        self.edge = self.adj

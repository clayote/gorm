# This file is part of gorm, an object relational mapper for versioned graphs.
# Copyright (C) 2014 Zachary Spector.
import networkx
from networkx.exception import NetworkXError
from collections import MutableMapping
from gorm.xjson import ismutable, JSONWrapper


class GraphMapping(MutableMapping):
    """Mapping for graph attributes"""
    def __init__(self, graph):
        """Initialize private dict and store pointers to the graph and ORM"""
        self.graph = graph
        self.gorm = graph.gorm

    def __iter__(self):
        """Iterate over the keys that are set"""
        seen = set()
        for (branch, rev) in self.gorm._active_branches():
            for k in self.gorm.db.graph_val_keys(
                    self.graph.name,
                    branch,
                    rev
            ):
                if k not in seen:
                    yield k
                seen.add(k)

    def __contains__(self, k):
        """Do I have a value for this key right now?"""
        try:
            self._get(k)
            return True
        except KeyError:
            return False

    def __len__(self):
        """Number of set keys"""
        n = 0
        for k in iter(self):
            n += 1
        return n

    def _get(self, key):
        """Just load value from database and return"""
        for (branch, rev) in self.gorm._active_branches():
            try:
                return self.gorm.db.graph_val_get(
                    self.graph.name,
                    key,
                    branch,
                    rev
                )
            except KeyError:
                continue
        raise KeyError("Key is not set, ever")

    def __getitem__(self, key):
        """If key is 'graph', return myself as a dict, else get the present
        value of the key and return that

        """
        if key == 'graph':
            return dict(self)
        for (branch, rev) in self.gorm._active_branches():
            try:
                r = self.gorm.db.graph_val_get(
                    self.graph.name,
                    key,
                    branch,
                    rev
                )
                if ismutable(r):
                    return JSONWrapper(self, key)
                else:
                    return r
            except KeyError:
                continue
        raise KeyError("key {} is not set, ever".format(key))

    def __setitem__(self, key, value):
        """Set key=value at the present branch and revision"""
        branch = self.gorm.branch
        rev = self.gorm.rev
        self.gorm.db.graph_val_set(
            self.graph.name,
            key,
            branch,
            rev,
            value
        )

    def __delitem__(self, key):
        """Indicate that the key has no value at this time"""
        branch = self.gorm.branch
        rev = self.gorm.rev
        self.gorm.db.graph_val_del(
            self.graph.name,
            key,
            branch,
            rev
        )

    def clear(self):
        """Delete everything"""
        for k in iter(self):
            del self[k]

    def __repr__(self):
        """Looks like a dictionary."""
        return repr(dict(self))

    def update(self, other):
        """Version of ``update`` that doesn't clobber the database so much"""
        iteratr = (
            other.iteritems
            if hasattr(other, 'iteritems')
            else other.items
        )
        for (k, v) in iteratr():
            if (
                    k not in self or
                    self[k] != v
            ):
                self[k] = v


class Node(GraphMapping):
    """Mapping for node attributes"""

    def __init__(self, graph, node):
        """Store name and graph"""
        self.graph = graph
        self.gorm = graph.gorm
        self.node = node

    def __iter__(self):
        """Iterate over those keys that are set at the moment

        """
        return self.gorm.db.node_val_keys(
            self.graph.name,
            self.node,
            self.gorm.branch,
            self.gorm.rev
        )

    def _get(self, key):
        return self.gorm.db.node_val_get(
            self.graph.name,
            self.node,
            key,
            self.gorm.branch,
            self.gorm.rev
        )

    def __getitem__(self, key):
        """Get the value of the key at the present branch and rev"""
        r = self._get(key)
        if ismutable(r):
            return JSONWrapper(self, key)
        else:
            return r

    def __setitem__(self, key, value):
        """Set key=value at the present branch and rev. Overwrite if
        necessary.

        """
        self.gorm.db.node_val_set(
            self.graph.name,
            self.node,
            key,
            self.gorm.branch,
            self.gorm.rev,
            value
        )

    def __delitem__(self, key):
        """Set the key's value to NULL, indicating it should be ignored
        now and in future revs

        """
        self.gorm.db.node_val_del(
            self.graph.name,
            self.node,
            key,
            self.gorm.branch,
            self.gorm.rev
        )

    def clear(self):
        """Delete everything"""
        for k in self:
            del self[k]


class Edge(GraphMapping):
    """Mapping for edge attributes"""
    def __init__(self, graph, nodeA, nodeB, idx=0):
        """Store the graph, the names of the nodes, and the index.

        For non-multigraphs the index is always 0.

        """
        self.graph = graph
        self.gorm = graph.gorm
        self.nodeA = nodeA
        self.nodeB = nodeB
        self.idx = idx

    def __iter__(self):
        """Yield those keys that have a value"""
        return self.gorm.db.edge_val_keys(
            self.graph.name,
            self.nodeA,
            self.nodeB,
            self.idx,
            self.gorm.branch,
            self.gorm.rev
        )

    def _get(self, key):
        return self.gorm.db.edge_val_get(
            self.graph.name,
            self.nodeA,
            self.nodeB,
            self.idx,
            key,
            self.gorm.branch,
            self.gorm.rev
        )

    def __getitem__(self, key):
        """Return the present value of the key, or raise KeyError if it's
        unset

        """
        r = self._get(key)
        if ismutable(r):
            return JSONWrapper(self, key)
        else:
            return r

    def __setitem__(self, key, value):
        """Set a database record to say that key=value at the present branch
        and revision

        """
        self.gorm.db.edge_val_set(
            self.graph.name,
            self.nodeA,
            self.nodeB,
            self.idx,
            key,
            self.gorm.branch,
            self.gorm.rev,
            value
        )

    def __delitem__(self, key):
        """Set the key's value to NULL, such that it is not yielded by
        ``__iter__``

        """
        self.gorm.db.edge_val_del(
            self.graph.name,
            self.nodeA,
            self.nodeB,
            self.idx,
            key,
            self.gorm.branch,
            self.gorm.rev
        )

    def clear(self):
        """Delete everything"""
        for k in self:
            del self[k]


class GraphNodeMapping(GraphMapping):
    """Mapping for nodes in a graph"""
    def __init__(self, graph):
        self.graph = graph
        self.gorm = graph.gorm

    def __iter__(self):
        """Iterate over the names of the nodes"""
        seen = set()
        for (branch, rev) in self.gorm._active_branches():
            for node in self.gorm.db.nodes_extant(
                    self.graph.name, branch, rev
            ):
                if node not in seen:
                    yield node
                seen.add(node)

    def __contains__(self, node):
        """Return whether the node exists presently"""
        return self.gorm.db.node_exists(
            self.graph.name,
            node,
            self.gorm.branch,
            self.gorm.rev
        )

    def __len__(self):
        """How many nodes exist right now?"""
        n = 0
        for node in iter(self):
            n += 1
        return n

    def __getitem__(self, node):
        """If the node exists at present, return it, else throw KeyError"""
        if node not in self:
            raise KeyError("Node doesn't exist")
        return Node(self.graph, node)

    def __setitem__(self, node, dikt):
        """Only accept dict-like values for assignment. These are taken to be
        dicts of node attributes, and so, a new GraphNodeMapping.Node
        is made with them, perhaps clearing out the one already there.

        """
        self.gorm.db.exist_node(
            self.graph.name,
            node,
            self.gorm.branch,
            self.gorm.rev,
            True
        )
        n = Node(self.graph, node)
        n.clear()
        n.update(dikt)

    def __delitem__(self, node):
        """Indicate that the given node no longer exists"""
        if node not in self:
            raise KeyError("No such node")
        self.gorm.db.exist_node(
            self.graph.name,
            node,
            self.gorm.branch,
            self.gorm.rev,
            False
        )

    def __eq__(self, other):
        """Compare values cast into dicts.

        As I serve the custom Node class, rather than dicts like
        networkx normally would, the normal comparison operation would
        not let you compare my nodes with regular networkx
        nodes-that-are-dicts. So I cast my nodes into dicts for this
        purpose, and cast the other argument's nodes the same way, in
        case it is a gorm graph.

        """
        if not hasattr(other, 'keys'):
            return False
        if set(self.keys()) != set(other.keys()):
            return False
        for k in self.keys():
            if dict(self[k]) != dict(other[k]):
                return False
        return True


class GraphEdgeMapping(GraphMapping):
    """Provides an adjacency mapping and possibly a predecessor mapping
    for a graph.

    """
    def __init__(self, graph):
        """Store the graph"""
        self.graph = graph
        self.gorm = graph.gorm

    def __eq__(self, other):
        """Compare dictified versions of the edge mappings within me.

        As I serve custom Predecessor or Successor classes, which
        themselves serve the custom Edge class, I wouldn't normally be
        comparable to a networkx adjacency dictionary. Converting
        myself and the other argument to dicts allows the comparison
        to work anyway.

        """
        if not hasattr(other, 'keys'):
            return False
        myks = set(self.keys())
        if myks != set(other.keys()):
            return False
        for k in myks:
            if dict(self[k]) != dict(other[k]):
                return False
        return True

    def __iter__(self):
        return self.gorm.db.edges_extant(
            self.graph.name,
            self.gorm.branch,
            self.gorm.rev
        )


class AbstractSuccessors(GraphEdgeMapping):
    def __init__(self, container, nodeA):
        """Store container and node"""
        self.container = container
        self.graph = container.graph
        self.gorm = self.graph.gorm
        self.nodeA = nodeA

    def __iter__(self):
        """Iterate over node IDs that have an edge with my nodeA"""
        return self.gorm.db.nodeBs(
            self.graph.name,
            self.nodeA,
            self.gorm.branch,
            self.gorm.rev
        )

    def __contains__(self, nodeB):
        """Is there an edge leading to ``nodeB`` at the moment?"""
        for i in self.gorm.db.multi_edges(
                self.graph.name,
                self.nodeA,
                nodeB,
                self.gorm.branch,
                self.gorm.rev
        ):
            return True
        return False

    def __len__(self):
        """How many nodes touch an edge shared with my nodeA?"""
        n = 0
        for nodeB in iter(self):
            n += 1
        return n

    def __getitem__(self, nodeB):
        """Get the edge between my nodeA and the given node"""
        return Edge(self.graph, self.nodeA, nodeB)

    def __setitem__(self, nodeB, value):
        """Set the edge between my nodeA and the given nodeB to the given
        value, a mapping.

        """
        self.gorm.db.exist_edge(
            self.graph.name,
            self.nodeA,
            nodeB,
            0,
            self.gorm.branch,
            self.gorm.rev,
            True
        )
        e = self[nodeB]
        e.clear()
        e.update(value)

    def __delitem__(self, nodeB):
        """Remove the edge between my nodeA and the given nodeB"""
        (nodeA, nodeB) = self._order_nodes(nodeB)
        self.gorm.db.exist_edge(
            self.graph.name,
            nodeA,
            nodeB,
            0,
            self.gorm.branch,
            self.gorm.rev,
            False
        )

    def clear(self):
        """Delete every edge with origin at my nodeA"""
        for nodeB in self:
            del self[nodeB]


class GraphSuccessorsMapping(GraphEdgeMapping):
    """Mapping for Successors (itself a MutableMapping)"""
    class Successors(AbstractSuccessors):
        def _order_nodes(self, nodeB):
            if nodeB < self.nodeA:
                return (nodeB, self.nodeA)
            else:
                return (self.nodeA, nodeB)

    def __getitem__(self, nodeA):
        """If the node exists, return a Successors instance for it"""
        if self.gorm.db.node_exists(
                self.graph.name,
                nodeA,
                self.gorm.branch,
                self.gorm.rev
        ):
            return self.Successors(self, nodeA)
        raise KeyError("No such node")

    def __setitem__(self, nodeA, val):
        """Wipe out any edges presently emanating from nodeA and replace them
        with those described by val

        """
        sucs = self.Successors(self, nodeA)
        sucs.clear()
        sucs.update(val)

    def __delitem__(self, nodeA):
        """Wipe out edges emanating from nodeA"""
        self.Successors(self, nodeA).clear()

    def __iter__(self):
        """Iterate over nodes that have at least one outgoing edge"""
        return self.gorm.db.edges_extant(
            self.graph.name,
            self.gorm.branch,
            self.gorm.rev
        )

    def __contains__(self, nodeA):
        """Does this node exist, and does it have at least one outgoing
        edge?

        """
        for b in self.gorm.db.nodeBs(
                self.graph.name,
                nodeA,
                self.gorm.branch,
                self.gorm.rev
        ):
            return True
        return False


class DiGraphSuccessorsMapping(GraphSuccessorsMapping):
    class Successors(AbstractSuccessors):
        def _order_nodes(self, nodeB):
            return (self.nodeA, nodeB)


class DiGraphPredecessorsMapping(GraphEdgeMapping):
    """Mapping for Predecessors instances, which map to Edges that end at
    the nodeB provided to this

    """
    def __contains__(self, nodeB):
        for a in self.gorm.db.nodeAs(
                self.graph.name,
                nodeB,
                self.gorm.branch,
                self.gorm.rev
        ):
            return True
        return False

    def __getitem__(self, nodeB):
        """Return a Predecessors instance for edges ending at the given
        node

        """
        if nodeB not in self:
            raise KeyError("No edges available")
        return self.Predecessors(self, nodeB)

    def __setitem__(self, nodeB, val):
        """Interpret ``val`` as a mapping of edges that end at ``nodeB``"""
        preds = self.Predecessors(self, nodeB)
        preds.clear()
        preds.update(val)

    def __delitem__(self, nodeB):
        """Delete all edges ending at ``nodeB``"""
        self.Predecessors(self, nodeB).clear()

    def __iter__(self):
        """Iterate over nodes with at least one edge leading to them"""
        return self.gorm.db.nodeBs(
            self.graph.name,
            self.nodeA,
            self.gorm.branch,
            self.gorm.rev
        )

    class Predecessors(GraphEdgeMapping):
        """Mapping of Edges that end at a particular node"""
        def __init__(self, container, nodeB):
            """Store container and node ID"""
            self.container = container
            self.graph = container.graph
            self.gorm = self.graph.gorm
            self.nodeB = nodeB

        def __iter__(self):
            """Iterate over the edges that exist at the present (branch, rev)

            """
            return self.gorm.db.nodeAs(
                self.graph.name,
                self.nodeB,
                self.gorm.branch,
                self.gorm.rev
            )

        def __contains__(self, nodeA):
            """Is there an edge from ``nodeA`` at the moment?"""
            for i in self.gorm.db.multi_edges(
                    self.graph.name,
                    self.nodeA,
                    self.nodeB,
                    self.gorm.branch,
                    self.gorm.rev
            ):
                return True
            return False

        def __len__(self):
            """How many edges exist at this rev of this branch?"""
            n = 0
            for nodeA in iter(self):
                n += 1
            return n

        def __getitem__(self, nodeA):
            """Get the edge from the given node to mine"""
            return Edge(self.graph, nodeA, self.nodeB)

        def __setitem__(self, nodeA, value):
            """Use ``value`` as a mapping of edge attributes, set an edge from the
            given node to mine.

            """
            self.gorm.db.exist_edge(
                self.graph.name,
                self.nodeA,
                self.nodeB,
                0,
                self.gorm.branch,
                self.gorm.rev,
                True
            )
            e = self._getsub(nodeA)
            e.clear()
            e.update(value)

        def __delitem__(self, nodeA):
            """Unset the existence of the edge from the given node to mine"""
            self.gorm.db.exist_edge(
                self.graph.name,
                self.nodeA,
                self.nodeB,
                0,
                self.gorm.branch,
                self.gorm.rev,
                False
            )


class MultiEdges(GraphEdgeMapping):
    """Mapping of Edges between two nodes"""
    def __init__(self, graph, nodeA, nodeB):
        """Store graph and node IDs"""
        self.graph = graph
        self.gorm = graph.gorm
        self.nodeA = nodeA
        self.nodeB = nodeB

    def __iter__(self):
        return self.gorm.db.multi_edges(
            self.graph.name,
            self.nodeA,
            self.nodeB,
            self.gorm.branch,
            self.gorm.rev
        )

    def __len__(self):
        """How many edges currently connect my two nodes?"""
        n = 0
        for idx in iter(self):
            n += 1
        return n

    def __contains__(self, i):
        return self.gorm.db.edge_exists(
            self.graph.name,
            self.nodeA,
            self.nodeB,
            i,
            self.gorm.branch,
            self.gorm.rev
        )

    def __getitem__(self, idx):
        """Get an Edge with a particular index, if it exists at the present
        (branch, rev)

        """
        if idx not in self:
            raise KeyError("No edge at that index")
        return Edge(self.graph, self.nodeA, self.nodeB, idx)

    def __setitem__(self, idx, val):
        """Create an Edge at a given index from a mapping. Delete the existing
        Edge first, if necessary.

        """
        self.gorm.db.exist_edge(
            self.graph.name,
            self.nodeA,
            self.nodeB,
            idx,
            self.gorm.branch,
            self.gorm.rev
        )
        e = Edge(self.graph, self.nodeA, self.nodeB, idx)
        e.clear()
        e.update(val)

    def __delitem__(self, idx):
        """Delete the edge at a particular index"""
        e = Edge(self.graph, self.nodeA, self.nodeB, idx)
        if not e.exists:
            raise KeyError("No edge at that index")
        e.clear()

    def clear(self):
        """Delete all edges between these nodes"""
        for idx in self:
            del self[idx]


class MultiGraphSuccessorsMapping(GraphSuccessorsMapping):
    """Mapping of Successors that map to MultiEdges"""
    def __getitem__(self, nodeA):
        """If the node exists, return its Successors"""
        if not self.gorm._node_exists(self.graph.name, nodeA):
            raise KeyError("No such node")
        return self.Successors(self, nodeA)

    def __setitem__(self, nodeA, val):
        """Interpret ``val`` as a mapping of successors, and turn it into a
        proper Successors object for storage

        """
        r = self.Successors(self, nodeA)
        r.clear()
        r.update(val)

    def __delitem__(self, nodeA):
        """Disconnect this node from everything"""
        self.Successors(self, nodeA).clear()

    class Successors(AbstractSuccessors):
        """Edges succeeding a given node in a multigraph"""
        def _order_nodes(self, nodeB):
            if nodeB < self.nodeA:
                return(nodeB, self.nodeA)
            else:
                return (self.nodeA, nodeB)

        def __getitem__(self, nodeB):
            """If ``nodeB`` exists, return the edges to it"""
            for nodeA in self.gorm.db.nodeAs(
                    self.graph.name,
                    nodeB,
                    self.gorm.branch,
                    self.gorm.rev
            ):
                return MultiEdges(self.graph, *self._order_nodes(nodeB))
            raise KeyError("No edge")

        def __setitem__(self, nodeB, val):
            """Interpret ``val`` as a dictionary of edge attributes for edges
            between my ``nodeA`` and the given ``nodeB``

            """
            self[nodeB].update(val)

        def __delitem__(self, nodeB):
            """Delete all edges between my ``nodeA`` and the given ``nodeB``"""
            self[nodeB].clear()


class MultiDiGraphPredecessorsMapping(DiGraphPredecessorsMapping):
    """Version of DiGraphPredecessorsMapping for multigraphs"""
    class Predecessors(DiGraphPredecessorsMapping.Predecessors):
        """Predecessor edges from a given node"""
        def __getitem__(self, nodeA):
            """Get MultiEdges"""
            return MultiEdges(self.graph, nodeA, self.nodeB)

        def __setitem__(self, nodeA, val):
            self[nodeA].update(val)

        def __delitem__(self, nodeA):
            self[nodeA].clear()


class GormGraph(object):
    """Class giving the gorm graphs those methods they share in
    common.

    """
    def _init_atts(self, gorm, name):
        """Initialize the mappings that are the same for all gorm graphs"""
        self._name = name
        self.gorm = gorm
        self.graph = GraphMapping(self)
        self.node = GraphNodeMapping(self)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, v):
        raise TypeError("gorm graphs can't be renamed")

    def _and_previous(self):
        """Return a 4-tuple that will usually be (current branch, current
        revision - 1, current branch, current revision), unless
        current revision - 1 is before the start of the current
        branch, in which case the first element will be the parent
        branch.

        """
        branch = self.gorm.branch
        rev = self.gorm.rev
        (parent, parent_rev) = self.gorm.sql('parparrev', branch).fetchone()
        before_branch = parent if parent_rev == rev else branch
        return (before_branch, rev-1, branch, rev)

    def clear(self):
        """Remove all nodes and edges from the graph.

        Unlike the regular networkx implementation, this does *not*
        remove the graph's name. But all the other graph, node, and
        edge attributes go away.

        """
        self.adj.clear()
        self.node.clear()
        self.graph.clear()


class Graph(GormGraph, networkx.Graph):
    """A version of the networkx.Graph class that stores its state in a
    database.

    """
    def __init__(self, gorm, name, data=None, **attr):
        """Call ``_init_atts``, instantiate special mappings, convert ``data``
        argument, and then update graph attributes from kwargs.

        """
        self._init_atts(gorm, name)
        self.adj = GraphSuccessorsMapping(self)
        self.edge = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)


class DiGraph(GormGraph, networkx.DiGraph):
    """A version of the networkx.DiGraph class that stores its state in a
    database.

    """
    def __init__(self, gorm, name, data=None, **attr):
        """Call ``_init_atts``, instantiate special mappings, convert ``data``
        argument, and then update graph attributes from kwargs.

        """
        self._init_atts(gorm, name)
        self.adj = DiGraphSuccessorsMapping(self)
        self.pred = DiGraphPredecessorsMapping(self)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)

    def remove_edge(self, u, v):
        """Version of remove_edge that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        try:
            del self.succ[u][v]
        except KeyError:
            raise NetworkXError(
                "The edge {}-{} is not in the graph.".format(u, v)
            )

    def remove_edges_from(self, ebunch):
        """Version of remove_edges_from that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        for e in ebunch:
            (u, v) = e[:2]
            if u in self.succ and v in self.succ[u]:
                del self.succ[u][v]

    def add_edge(self, u, v, attr_dict=None, **attr):
        """Version of add_edge that only writes to the database once"""
        if attr_dict is None:
            attr_dict = attr
        else:
            try:
                attr_dict.update(attr)
            except AttributeError:
                raise NetworkXError(
                    "The attr_dict argument must be a dictionary."
                )
        datadict = self.adj[u].get(v, {})
        datadict.update(attr_dict)
        if u not in self.node:
            self.node[u] = {}
        if v not in self.node:
            self.node[v] = {}
        self.succ[u][v] = datadict
        assert(
            u in self.succ and
            v in self.succ[u]
        )

    def add_edges_from(self, ebunch, attr_dict=None, **attr):
        """Version of add_edges_from that only writes to the database once"""
        if attr_dict is None:
            attr_dict = attr
        else:
            try:
                attr_dict.update(attr)
            except AttributeError:
                raise NetworkXError(
                    "The attr_dict argument must be a dict."
                )
        for e in ebunch:
            ne = len(e)
            if ne == 3:
                u, v, dd = e
                assert hasattr(dd, "update")
            elif ne == 2:
                u, v = e
                dd = {}
            else:
                raise NetworkXError(
                    "Edge tupse {} must be a 2-tuple or 3-tuple.".format(e)
                )
            if u not in self.node:
                self.node[u] = {}
            if v not in self.node:
                self.node[v] = {}
            datadict = self.adj.get(u, {}).get(v, {})
            datadict.update(attr_dict)
            datadict.update(dd)
            self.succ[u][v] = datadict
            assert(u in self.succ)
            assert(v in self.succ[u])


class MultiGraph(GormGraph, networkx.MultiGraph):
    """A version of the networkx.MultiGraph class that stores its state in a
    database.

    """
    def __init__(self, gorm, name, data=None, **attr):
        """Call ``_init_atts``, instantiate special mappings, convert ``data``
        argument, and then update graph attributes from kwargs.

        """
        self._init_atts(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        self.edge = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)


class MultiDiGraph(GormGraph, networkx.MultiDiGraph):
    """A version of the networkx.MultiDiGraph class that stores its state in a
    database.

    """
    def __init__(self, gorm, name, data=None, **attr):
        """Call ``_init_atts``, instantiate special mappings, convert ``data``
        argument, and then update graph attributes from kwargs.

        """
        self._init_atts(gorm, name)
        self.adj = MultiGraphSuccessorsMapping(gorm, name)
        self.pred = MultiDiGraphPredecessorsMapping(gorm, name)
        self.succ = self.adj
        if data is not None:
            networkx.convert.to_networkx_graph(data, create_using=self)
        self.graph.update(attr)

    def remove_edge(self, u, v, key=None):
        """Version of remove_edge that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        try:
            d = self.adj[u][v]
        except KeyError:
            raise NetworkXError(
                "The edge {}-{} is not in the graph.".format(u, v)
            )
        if key is None:
            d.popitem()
        else:
            try:
                del d[key]
            except KeyError:
                raise NetworkXError(
                    "The edge {}-{} with key {} is not in the graph.".format
                    (u, v, key)
                )
        if len(d) == 0:
            del self.succ[u][v]

    def remove_edges_from(self, ebunch):
        """Version of remove_edges_from that's much like normal networkx but only
        deletes once, since the database doesn't keep separate adj and
        succ mappings

        """
        for e in ebunch:
            (u, v) = e[:2]
            if u in self.succ and v in self.succ[u]:
                del self.succ[u][v]

    def add_edge(self, u, v, key=None, attr_dict=None, **attr):
        """Version of add_edge that only writes to the database once."""
        if attr_dict is None:
            attr_dict = attr
        else:
            try:
                attr_dict.update(attr)
            except AttributeError:
                raise NetworkXError(
                    "The attr_dict argument must be a dictionary."
                )
        if u not in self.node:
            self.node[u] = {}
        if v not in self.node:
            self.node[v] = {}
        if v in self.succ[u]:
            keydict = self.adj[u][v]
            if key is None:
                key = len(keydict)
                while key in keydict:
                    key += 1
            datadict = keydict.get(key, {})
            datadict.update(attr_dict)
            keydict[key] = datadict
        else:
            if key is None:
                key = 0
            datadict = {}
            datadict.update(attr_dict)
            keydict = {key: datadict}
            self.succ[u][v] = keydict

# This file is part of gorm, an object relational mapper for versioned graphs.
# Copyright (C) 2014 Zachary Spector.
from .graph import (
    Graph,
    DiGraph,
    MultiGraph,
    MultiDiGraph,
)
from .query import QueryEngine


class GraphNameError(KeyError):
    pass


class ORM(object):
    """Instantiate this with the same string argument you'd use for a
    SQLAlchemy ``create_engine`` call. This will be your interface to
    gorm.

    """
    def __init__(
            self,
            dbstring,
            alchemy=True,
            connect_args={},
            obranch=None,
            orev=None,
            query_engine_class=QueryEngine
    ):
        """Make a SQLAlchemy engine if possible, else a sqlite3 connection. In
        either case, begin a transaction.

        """
        self.db = query_engine_class(dbstring, connect_args, alchemy)
        self._obranch = obranch
        self._orev = orev
        self._branches = {}

    def __enter__(self):
        """Enable the use of the ``with`` keyword"""
        return self

    def __exit__(self, *args):
        """Alias for ``close``"""
        self.close()

    def _havebranch(self, b):
        """Private use. Checks that the branch is known about."""
        if b in self._branches:
            return True
        return self.db.have_branch(b)

    def is_parent_of(self, parent, child):
        """Return whether ``child`` is a branch descended from ``parent`` at
        any remove.

        """
        # trivial cases
        if child in self._branches and self._branches[child][0] == parent:
            return True
        elif child == parent:
            return False
        # I will be recursing a lot so just cache all the branch info
        self._childbranch = {}
        self._ancestry = {}
        for (branch, parent, parent_rev) in self.db.all_branches():
            self._branches[branch] = (parent, parent_rev)
            self._childbranch[parent] = branch

        self._ancestry[child] = set([parent])
        lineage = self._ancestry[child]

        def recurse(oneparent):
            if oneparent in lineage:
                return True
            if oneparent not in self._branches:
                return False
            if self._branches[oneparent][0] in lineage:
                return True
            lineage.add(oneparent)
            return recurse(self._branches[oneparent][0])
        return recurse(child)

    @property
    def branch(self):
        """Return the global value ``branch``, or ``self._obranch`` if it's
        set

        """
        if self._obranch is not None:
            return self._obranch
        return self.db.globl['branch']

    @branch.setter
    def branch(self, v):
        """Set the global value ``branch`` and note that the branch's (parent,
        parent_rev) are the (branch, tick) set previously

        """
        curbranch = self.branch
        currev = self.rev
        if not self._havebranch(v):
            # assumes the present revision in the parent branch has
            # been finalized.
            self.db.new_branch(v, curbranch, currev)
        if v == 'master':
            return
        # make sure I'll end up within the revision range of the
        # destination branch
        parrev = self.db.parrev(v)
        if currev < parrev:
            raise ValueError(
                "Tried to jump to branch {br}, which starts at revision {rv}. "
                "Go to rev {rv} or later to use this branch.".format(
                    br=v,
                    rv=currev
                )
            )
        self.db.globl['branch'] = v

    @property
    def rev(self):
        """Return the global value ``rev``, or ``self._orev`` if that's set"""
        if self._orev is not None:
            return self._orev
        return self.db.globl['rev']

    @rev.setter
    def rev(self, v):
        """Set the global value ``rev``, first checking that it's not before
        the start of this branch. If it is, also go to the parent
        branch.

        """
        # first make sure the cursor is not before the start of this branch
        branch = self.branch
        if branch != 'master':
            (parent, parent_rev) = self.db.parparrev(branch)
            if v < int(parent_rev):
                raise ValueError(
                    "The revision number {revn} "
                    "occurs before the start of "
                    "the branch {brnch}".format(revn=v, brnch=branch)
                )
        self.db.globl['rev'] = v
        assert(self.rev == v)

    def commit(self):
        """Alias of ``self.db.commit``"""
        self.db.commit()

    def close(self):
        """Alias of ``self.db.close``"""
        self.db.close()

    def initdb(self):
        """Alias of ``self.db.initdb``"""
        self.db.initdb()

    def _init_graph(self, name, type_s='Graph'):
        if self.db.have_graph(name):
            raise GraphNameError("Already have a graph by that name")
        self.db.new_graph(name, type_s)

    def new_graph(self, name, data=None, **attr):
        """Return a new instance of type Graph, initialized with the given
        data if provided.

        """
        self._init_graph(name, 'Graph')
        return Graph(self, name, data, **attr)

    def new_digraph(self, name, data=None, **attr):
        """Return a new instance of type DiGraph, initialized with the given
        data if provided.

        """
        self._init_graph(name, 'DiGraph')
        return DiGraph(self, name, data, **attr)

    def new_multigraph(self, name, data=None, **attr):
        """Return a new instance of type MultiGraph, initialized with the given
        data if provided.

        """
        self._init_graph(name, 'MultiGraph')
        return MultiGraph(self, name, data, **attr)

    def new_multidigraph(self, name, data=None, **attr):
        """Return a new instance of type MultiDiGraph, initialized with the given
        data if provided.

        """
        self._init_graph(name, 'MultiDiGraph')
        return MultiDiGraph(self, name, data, **attr)

    def get_graph(self, name):
        """Return a graph previously created with ``new_graph``,
        ``new_digraph``, ``new_multigraph``, or
        ``new_multidigraph``

        """
        graphtypes = {
            'Graph': Graph,
            'DiGraph': DiGraph,
            'MultiGraph': MultiGraph,
            'MultiDiGraph': MultiDiGraph
        }
        type_s = self.db.graph_type(name)
        if type_s not in graphtypes:
            raise GraphNameError("I don't know of a graph named {}".format(name))
        return graphtypes[type_s](self, name)

    def del_graph(self, name):
        """Remove all traces of a graph's existence from the database"""
        # make sure the graph exists before deleting anything
        self.get_graph(name)
        self.db.del_graph(name)

    def _active_branches(self, branch=None, rev=None):
        """Private use. Iterate over (branch, rev) pairs, where the branch is
        a descendant of the previous (starting with whatever branch is
        presently active and ending at 'master'), and the rev is the
        latest revision in the branch that matters.

        """
        if branch is None:
            branch = self.branch
        if rev is None:
            rev = self.rev
        for (b, r) in self.db.active_branches(branch, rev):
            yield (b, r)

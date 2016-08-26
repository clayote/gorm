import unittest
from copy import deepcopy
import gorm
import networkx as nx
from networkx.generators.atlas import graph_atlas_g


testkvs = [0, 1, 10, 10**10, 10**10**4, 'spam', 'eggs', 'ham',  '💧', '🔑', '𐦖',('spam', 'eggs', 'ham'), ['spam', 'eggs', 'ham']]
testdata = []
for k in testkvs:
    for v in testkvs:
        testdata.append((k, v))
testdata.append(('lol', deepcopy(testdata)))


class GormTest(unittest.TestCase):
    def setUp(self):
        self.engine = gorm.ORM('sqlite:///:memory:')
        self.engine.initdb()
        self.graphmakers = (self.engine.new_graph, self.engine.new_digraph, self.engine.new_multigraph, self.engine.new_multidigraph)

    def tearDown(self):
        self.engine.close()

    def test_branch_lineage(self):
        """Create some branches of history and check that gorm remembers where
        each came from and what happened in each.

        """
        g = self.engine.new_graph('branch_lineage')
        g.add_node(0)
        g.add_node(1)
        g.add_edge(0, 1)
        self.engine.rev = 1
        self.engine.branch = 'no_edge'
        g.remove_edge(0, 1)
        self.assertNotIn(0, g.edge)
        self.engine.branch = 'triangle'
        g.add_node(2)
        g.add_edge(0, 1)
        g.add_edge(1, 2)
        g.add_edge(2, 0)
        self.engine.branch = 'square'
        self.engine.rev = 2
        g.remove_edge(2, 0)
        g.add_node(3)
        g.add_edge(2, 3)
        g.add_edge(3, 0)
        self.engine.branch = 'nothing'
        del g.node[0]
        del g.node[1]
        self.assertTrue(self.engine.is_parent_of('master', 'no_edge'))
        self.assertTrue(self.engine.is_parent_of('master', 'triangle'))
        self.assertTrue(self.engine.is_parent_of('master', 'nothing'))
        self.assertTrue(self.engine.is_parent_of('no_edge', 'triangle'))
        self.assertTrue(self.engine.is_parent_of('square', 'nothing'))
        self.assertFalse(self.engine.is_parent_of('nothing', 'master'))
        self.assertFalse(self.engine.is_parent_of('triangle', 'no_edge'))
        self.engine.branch = 'master'
        self.assertIn(0, g.node)
        self.assertIn(1, g.node)
        self.assertIn(0, g.edge)
        self.assertIn(1, g.edge[0])
        self.engine.rev = 0

        def badjump():
            self.engine.branch = 'no_edge'
        self.assertRaises(ValueError, badjump)
        self.engine.rev = 2
        self.engine.branch = 'no_edge'
        self.assertNotIn(0, g.edge)
        self.engine.branch = 'triangle'
        self.assertIn(2, g.node)
        for orig in (0, 1, 2):
            for dest in (0, 1, 2):
                if orig == dest:
                    continue
                self.assertIn(orig, g.edge)
                self.assertIn(dest, g.edge[orig])
        self.engine.branch = 'square'
        self.assertNotIn(3, g.node)
        self.engine.rev = 2
        self.assertIn(3, g.node)
        self.assertIn(1, g.edge[0])
        self.assertIn(2, g.edge[1])
        self.assertIn(3, g.edge[2])
        self.assertIn(0, g.edge[3])
        self.engine.branch = 'nothing'
        for node in (0, 1, 2):
            self.assertNotIn(node, g.node)
            self.assertNotIn(node, g.edge)
        self.engine.branch = 'master'
        self.engine.rev = 0
        self.assertIn(0, g.node)
        self.assertIn(1, g.node)
        self.assertIn(0, g.edge)
        self.assertIn(1, g.edge[0])

    def test_graph_storage(self):
        """Test that all the graph types can store and retrieve key-value pairs
        for the graph as a whole.

        """
        for graphmaker in self.graphmakers:
            g = graphmaker('testgraph')
            for (k, v) in testdata:
                g.graph[k] = v
                self.assertIn(k, g.graph)
                self.assertEqual(g.graph[k], v)
            self.engine.del_graph('testgraph')

    def test_node_storage(self):
        """Test that all the graph types can store and retrieve key-value
        pairs for particular nodes."""
        pass

    def test_edge_storage(self):
        """Test that all the graph types can store and retrieve key-value
        pairs for particular edges.

        """
        pass

    def test_compiled_queries(self):
        """Make sure that the queries generated in SQLAlchemy are the same as
        those precompiled into SQLite.

        """
        from gorm.alchemy import Alchemist
        self.assertTrue(hasattr(self.engine.db, 'alchemist'))
        self.assertTrue(isinstance(self.engine.db.alchemist, Alchemist))
        from json import loads
        precompiled = loads(
            open(self.engine.db.json_path + '/sqlite.json', 'r').read()
        )
        self.assertEqual(
            precompiled.keys(), self.engine.db.alchemist.sql.keys()
        )
        for (k, query) in precompiled.items():
            self.assertEqual(
                query,
                str(
                    self.engine.db.alchemist.sql[k]
                )
            )


if __name__ == '__main__':
    unittest.main()

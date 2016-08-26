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
        g.add_node('n0')
        g.add_node('n1')
        g.add_edge('n0', 'n1')
        self.engine.rev = 1
        self.engine.branch = 'no_edge'
        g.remove_edge('n0', 'n1')
        self.engine.branch = 'triangle'
        g.add_node('n2')
        g.add_edge('n0', 'n1')
        g.add_edge('n1', 'n2')
        g.add_edge('n2', 'n0')
        self.engine.branch = 'square'
        self.engine.rev = 2
        g.remove_edge('n2', 'n0')
        g.add_node('n3')
        g.add_edge('n2', 'n3')
        g.add_edge('n3', 'n0')
        self.engine.branch = 'nothing'
        del g.node['n0']
        del g.node['n1']
        self.assertTrue(self.engine.is_parent_of('master', 'no_edge'))
        self.assertTrue(self.engine.is_parent_of('master', 'triangle'))
        self.assertTrue(self.engine.is_parent_of('master', 'nothing'))
        self.assertTrue(self.engine.is_parent_of('no_edge', 'triangle'))
        self.assertTrue(self.engine.is_parent_of('square', 'nothing'))
        self.assertFalse(self.engine.is_parent_of('nothing', 'master'))
        self.assertFalse(self.engine.is_parent_of('triangle', 'no_edge'))
        self.engine.branch = 'master'
        self.assertIn('n0', g.node)
        self.assertIn('n1', g.node)
        self.assertIn('n0', g.edge)
        self.assertIn('n1', g.edge['n0'])
        self.engine.rev = 0

        def badjump():
            self.engine.branch = 'no_edge'
        self.assertRaises(ValueError, badjump)
        self.engine.rev = 2
        self.engine.branch = 'no_edge'
        self.assertNotIn('n0', g.edge)
        self.engine.branch = 'triangle'
        self.assertIn('n2', g.node)
        def triTest():
            for orig in ('n0', 'n1', 'n2'):
                for dest in ('n0', 'n1', 'n2'):
                    if orig == dest:
                        continue
                    self.assertIn(orig, g.edge)
                    self.assertIn(dest, g.edge[orig])
        triTest()
        self.engine.branch = 'square'
        triTest()
        self.assertNotIn('n3', g.node)
        self.engine.rev = 2
        self.assertIn('n3', g.node)
        self.assertIn('n1', g.edge['n0'])
        self.assertIn('n2', g.edge['n1'])
        self.assertIn('n3', g.edge['n2'])
        self.assertIn('n0', g.edge['n3'])
        self.engine.branch = 'nothing'
        for node in ('n0', 'n1', 'n2'):
            self.assertNotIn(node, g.node)
            self.assertNotIn(node, g.edge)
        self.engine.branch = 'master'
        self.engine.rev = 0
        self.assertIn('n0', g.node)
        self.assertIn('n1', g.node)
        self.assertIn('n0', g.edge)
        self.assertIn('n1', g.edge['n0'])

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

    def test_graph_atlas(self):
        """Test saving and loading all the graphs in the networkx graph
        atlas.

        """
        for g in graph_atlas_g():
            if int(g.name[1:]) % 100 == 0:
                print(g.name)
            gormg = self.engine.new_graph(g.name, g)
            for n in g.node:
                self.assertIn(n, gormg.node)
                self.assertEqual(g.node[n], gormg.node[n])
            for u in g.edge:
                for v in g.edge[u]:
                    self.assertIn(u, gormg.edge)
                    self.assertIn(v, gormg.edge[u])
                    self.assertEqual(g.edge[u][v], gormg.edge[u][v])


if __name__ == '__main__':
    unittest.main()

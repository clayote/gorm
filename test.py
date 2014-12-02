import unittest
import gorm
import networkx as nx
from networkx.generators.atlas import graph_atlas_g


class ManipTest(unittest.TestCase):
    def setUp(self):
        self.engine = gorm.ORM('sqlite:///:memory:')
        self.engine.initdb()

    def test_graph_atlas(self):
        """Test saving and loading all the graphs in the networkx graph
        atlas.

        """
        for g in graph_atlas_g():
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

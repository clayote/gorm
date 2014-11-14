import networkx as nx
from networkx.generators.atlas import graph_atlas_g
import unittest
import gorm


class GormTestCase(unittest.TestCase):
    def setUp(self):
        self.gorm = gorm.ORM('sqlite:///:memory:')
        self.gorm.initdb()

    def tearDown(self):
        self.gorm.close()

    def test_init_graph_atlas(self):
        """Test saving each of the graphs in the networkx graph atlas."""
        atlas = graph_atlas_g()
        for graph in atlas:
            method = (
                self.gorm.new_graph if isinstance(graph, nx.Graph) else
                self.gorm.new_digraph if isinstance(graph, nx.DiGraph) else
                self.gorm.new_multigraph if
                isinstance(graph, nx.MultiGraph) else
                self.gorm.new_multidigraph
            )
            gormgraph = method(graph.name, graph)
            self.assertEqual(graph.graph, gormgraph.graph)
            self.assertEqual(graph.node, gormgraph.node)
            # I can't directly compare the adjancency mappings because
            # gorm's edge table doesn't store any records for nodes
            # with no edges from them, and will thus appear empty {}
            # when the networkx graph would be eg. {0: {}}
            for u in graph.adj:
                for v in graph.adj[u]:
                    self.assertEqual(graph.adj[u][v], gormgraph.adj[u][v])


if __name__ == '__main__':
    unittest.main()

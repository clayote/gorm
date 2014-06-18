from networkx import Graph as NXGraph
from mappings import (
    GraphMapping,
    NodeMapping,
    EdgeMapping
)


class Graph(NXGraph):
    def __init__(self, gorm, name, data=None, **attr):
        self.graph = GraphMapping(gorm, name)
        

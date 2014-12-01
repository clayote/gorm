from gorm import ORM
from sqlite3 import connect
from os import remove
import networkx as nx
import matplotlib.pyplot as plt

try:
    remove('test.db')
except OSError:
    pass
conn = connect('test.db')
lol = nx.lollipop_graph(3,3)
orm = ORM('sqlite://')
orm.initdb()
pop = orm.new_graph('pop', lol)
assert(lol.edge == pop.edge)
orm.rev = 1
pop.add_edge(5, 3)
assert(lol.edge != pop.edge)
orm.rev = 0
assert(lol.edge == pop.edge)
orm.branch = 'test'
assert(lol.edge == pop.edge)
orm.rev = 1
assert(lol.edge == pop.edge)

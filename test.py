from gorm import ORM
from sqlite3 import connect
from os import remove
import networkx as nx
import matplotlib.pyplot as plt

def plot_and_show(G, fn):
    pos = nx.graphviz_layout(G, prog='twopi', args='')
    plt.figure(figsize=(8,8))
    nx.draw(G, pos, node_size=20, alpha=0.5, node_color='blue', with_labels=False)
    plt.axis('equal')
    plt.savefig(fn)
    plt.show()

try:
    remove('test.db')
except OSError:
    pass
conn = connect('test.db')
with ORM(connector=conn) as orm:
    orm.initdb()
    G = nx.balanced_tree(3, 5)
    plot_and_show(G, 'before.png')
    graph = orm.new_graph('balanced3,5', G)
    Gedge = set(G.edges())
    gedge = set(graph.edges())
    print(Gedge - gedge)
    print(gedge - Gedge)
    plot_and_show(graph, 'during.png')
conn.close()
conn = connect('test.db')
with ORM(connector=conn) as orm:
    plot_and_show(orm.get_graph('balanced3,5'), 'after.png')

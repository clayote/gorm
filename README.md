gorm
====

Object relational mapper for graphs with in-built revision control.


gorm serves its own special variants on the networkx graph classes: Graph, DiGraph, MultiGraph, and MultiDiGraph. Every change to them is stored in an SQL database.

This means you can keep multiple versions of one set of graphs and switch between them without the need to save, load, or run git-checkout. Just point the ORM at the correct branch and revision, and all of the graphs in the program will change. All the different branches and revisions remain in the database to be brought back when needed.

from setuptools import setup, find_packages
setup(
    name = "gorm",
    version = "0.1.3",
    packages = find_packages(),
    install_requires = ['networkx>=1.8.1'],
    author = "Zachary Spector",
    author_email = "zacharyspector@gmail.com",
    description = "An object-relational mapper serving database-backed versions of the standard networkx graph classes.",
    license = "GPL3",
    keywords = "orm graph networkx sql database",
    url = "https://github.com/LogicalDash/gorm"
)

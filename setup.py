from setuptools import setup
from setuptools.extension import Extension
from Cython.Build import cythonize
setup(
    name = "gorm",
    version = "0.10.0",
    packages = ["gorm"],
    install_requires = ['networkx>=1.9'],
    author = "Zachary Spector",
    author_email = "zacharyspector@gmail.com",
    description = "An object-relational mapper serving database-backed versions of the standard networkx graph classes.",
    license = "BSD",
    keywords = "orm graph networkx sql database",
    url = "https://github.com/LogicalDash/gorm",
    ext_modules=cythonize(["gorm/trique.pyx"]),
    package_dir={
        "gorm": "gorm"
    },
    package_data={
        "gorm": [
            "sqlite.json"
        ]
    }
)

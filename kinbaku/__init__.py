"""
Kinbaku
=======
Kinbaku is a graph database written in pure Python, made for storing,
updating and accessing large graphs directly on disk.
See https://kinbaku.readthedocs.io/en/latest/ for complete documentation.
"""

__version__ = "0.0.2"

from .graph import Graph

__all__ = ["Graph"]

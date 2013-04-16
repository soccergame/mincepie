"""
Lightweight MapReduce on Python

Yangqing Jia, jiayq@eecs.berkeley.edu
"""
__version__ = '0.1'

from . import launcher
from . import mapreducer
from . import mince

__all__ = ['launcher', 'mapreducer', 'mince']
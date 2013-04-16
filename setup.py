#!/usr/bin/env python

from distutils.core import setup

setup(
    name='mincepie',
    version='0.1.0',
    description='Simple MapReduce based on Python',
    author='Yangqing Jia',
    author_email='jiayq84@gmail.com',
    packages=['mincepie'],
    requires=['gflags'],
)

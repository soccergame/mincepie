#!/usr/bin/env python

from distutils.core import setup

setup(
    name='mincepie',
    version='0.0.1',
    description='Simple MapReduce based on Python',
    author='Yangqing Jia',
    author_email='jiayq84@gmail.com',
    packages=['mincepie'],
    install_requires=['python-gflags'],
)

#!/usr/bin/env python

from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().strip().splitlines()

# install
setup(
    name='kafka_producers',
    version='1.1',
    install_requires=requirements,
    packages=[
        'tornado_producer'
    ]
)


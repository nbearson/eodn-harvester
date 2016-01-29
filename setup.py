#!/usr/bin/env python3

from setuptools import setup

setup(name        = 'eodnharvester',
      version     = '0.2.3',
      description = "eodnharvester gathers scene data from eros, downloads availible scenes, uploads scenes to EODN depots and updates UNIS with availible exnodes.",
      author      = "Jeremy Musser",
      packages = ['eodnharvester', 'eodnharvester.sec'],
      package_data = { 'eodnharvester.sec': ['*.pem'] },
      include_package_data = True,
      install_requires=["python-daemon", "requests"],
      entry_points = {
          'console_scripts': [
              'eodnharvesterd = eodnharvester.app:main',
          ]
      },
  )

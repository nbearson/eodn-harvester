#!/usr/bin/env python

from setuptools import setup

setup(name        = 'eodnharvester',
      version     = '0.1.0',
      description = "eodnharvester gathers scene data from eros, downloads availible scenes, uploads scenes to EODN depots and updates UNIS with availible exnodes.",
      author      = "Jeremy Musser",
      scripts = ['harvest.py', 'reporter.py', 'settings.py'],
      entry_points = {
          'console_scripts': [
              'eodnharvesterd = harvest:main',
          ]
      },
  )

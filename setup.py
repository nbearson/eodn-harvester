#!/usr/bin/env python3

from setuptools import setup

setup(name        = 'eodnharvester',
      version     = '0.2.2',
      description = "eodnharvester gathers scene data from eros, downloads availible scenes, uploads scenes to EODN depots and updates UNIS with availible exnodes.",
      author      = "Jeremy Musser",
      scripts = ['app.py', 'reporter.py', 'settings.py', 'search.py', 'auth.py', 'entity.py', 'history.py', 'product.py'],
      install_requires=[
          "daemon",
          "futures"
          ],
      entry_points = {
          'console_scripts': [
              'eodnharvesterd = app:main',
          ]
      },
  )

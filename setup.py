#!/usr/bin/env python3
# =============================================================================
#  EODNHarvester
#
#  Copyright (c) 2015-2016, Trustees of Indiana University,
#  All rights reserved.
#
#  This software may be modified and distributed under the terms of the BSD
#  license.  See the COPYING file for details.
#
#  This software was created at the Indiana University Center for Research in
#  Extreme Scale Technologies (CREST).
# =============================================================================
from setuptools import setup

setup(name        = 'eodnharvester',
      version     = '0.2.3',
      description = "eodnharvester gathers scene data from eros, downloads availible scenes, uploads scenes to EODN depots and updates UNIS with availible exnodes.",
      author      = "Jeremy Musser",
      packages = ['eodnharvester', 'eodnharvester.sec'],
      package_data = { 'eodnharvester.sec': ['*.pem'] },
      include_package_data = True,
      install_requires=[
          "python-daemon",
          "requests",
          "netifaces"
      ],
      entry_points = {
          'console_scripts': [
              'eodnharvesterd = eodnharvester.app:main',
          ]
      },
  )

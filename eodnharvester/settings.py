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
import os

##################################
#                                #
#   Harvest Operation Settings   #
#                                #
##################################

HARVEST_NAME   = ""
VERBOSE        = False                    # boolean
DEBUG          = False                    # boolean

THREADS        = 1                        # int
MAX_RECONNECT  = 10                       # int
HARVEST_WINDOW = {"minutes": 5 }         # timedelta obj
WORKSPACE      = "/data"         # directory string




##################################
#                                #
#         USGS Settings          #
#                                #
##################################

USGS_HOST = "earthexplorer.usgs.gov" # hostname or ip
USERNAME  = "indianadlt"             # string
PASSWORD  = "indiana2014"            # string
TIMEOUT   = 40                       # int seconds


##################################
#                                #
#    Default Search Settings     #
#                                #
##################################

DATASET_NAME = "Landsat_8" # string
LOWER_LEFT = {
    "latitude":    24.52,  # double
    "longitude": -124.59   # double
    }
UPPER_RIGHT = {
    "latitude":   49.4,    # double
    "longitude": -66.95    # double
    }
MAX_RESULTS = 15           # int
SORT_ORDER  = "DESC"       # ASC or DESC string
NODE        = "EE"         # string


##################################
#                                #
#     Preservation Settings      #
#                                #
##################################

# TODO: maybe rename "PRESERVE" to "MIRROR"
PRESERVE_LOCALLY   = True                         # boolean
LOCAL_PRESERVE_DIR = "/data3/eodn/harvester-save" # directory string
LOCAL_QUARANTINE_DIR = "/data3/eodn/harvester-quarantine" # directory string

##################################
#                                #
#       Download Settings        #
#                                #
##################################

DOWNLOAD_CHUNKSIZE = 8192 # int



##################################
#                                #
#         LoRS Settings          #
#                                #
##################################

UNIS_HOST = "unis.crest.iu.edu"          # hostname or ip
UNIS_PORT = 8890                         # port as int
LoRS = {
    "duration": 24 * 30,
    "copies":   3,               # replication factor           int
    "depots":   20,              # number of depots             int
    "threads":  10,              # number of threads            int
    "size":     "10m",           # size of allocation           string
    "xndrc":    "/root/.xndrc"   # directory string
}



##################################
#                                #
#       Reporter Settings        #
#                                #
##################################

FORCE_EMAIL            = True
REPORT_HOUR            = 0 # Hour of day (0-23)
REPORT_EMAIL           = "dlt-news@crest.iu.edu"
VALIDATION_GRANULARITY = 1024

##################################
#                                #
#       Auth Related settings    #
#                                #
##################################
HARVESTER_ROOT = os.path.dirname(os.path.abspath(__file__)) + os.sep

USE_SSL = False
SSL_OPTIONS = {
    "key": HARVESTER_ROOT + "sec/dlt-client.pem",
    "cert": HARVESTER_ROOT + "sec/dlt-client.pem"
}

AUTH_FIELD = "secToken"
AUTH_VALUE = ["landsat"]

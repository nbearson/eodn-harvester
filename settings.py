

##################################
#                                #
#   Harvest Operation Settings   #
#                                #
##################################

VERBOSE        = False                    # boolean
DEBUG          = False                    # boolean

THREADS        = 5                        # int
MAX_RECONNECT  = 10                       # int
HARVEST_WINDOW = {"minutes": 30}          # timedelta obj
WORKSPACE      = "/data/jemusser"         # directory string
HISTORY_PATH   = WORKSPACE + "/hist.json" # directory and filename string




##################################
#                                #
#         USGS Settings          #
#                                #
##################################

USGS_HOST = "earthexplorer.usgs.gov" # hostname or ip
USERNAME  = "prblackwell"            # string
PASSWORD  = "g00d4USGS"              # string
TIMEOUT   = 40                       # int seconds


##################################
#                                #
#        Search Settings         #
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
#       Download Settings        #
#                                #
##################################

DOWNLOAD_CHUNKSIZE = 8192 # int



##################################
#                                #
#         LoRS Settings          #
#                                #
##################################

UNIS_HOST = "localhost"          # hostname or ip
UNIS_PORT = 8888                 # port as int
LoRS = {
    "duration": 10,              # allocation duration in hours int
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

REPORT_PERIOD = { "minutes": 30 }       # timedelta obj
REPORT_EMAIL  = "jemusser@umail.iu.edu" # email string

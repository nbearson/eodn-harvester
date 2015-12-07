

##################################
#                                #
#   Harvest Operation Settings   #
#                                #
##################################

HARVEST_NAME   = "Test"
VERBOSE        = False                    # boolean
DEBUG          = False                    # boolean

THREADS        = 1                        # int
MAX_RECONNECT  = 10                       # int
HARVEST_WINDOW = {"minutes": 15 }         # timedelta obj
WORKSPACE      = "/data/jemusser"         # directory string




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
#       Download Settings        #
#                                #
##################################

DOWNLOAD_CHUNKSIZE = 8192 # int



##################################
#                                #
#         LoRS Settings          #
#                                #
##################################

#UNIS_HOST = "dev.incntre.iu.edu"
UNIS_HOST = "localhost"          # hostname or ip
UNIS_PORT = 8888                 # port as int
LoRS = {
    #"duration": 30 * 24,
    "duration": 10,               # allocation duration in hours int
    "copies":   1,               # replication factor           int
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

#REPORT_HOUR           = 0
REPORT_HOUR            = 14 # Hour of day (0-23)
#REPORT_EMAIL          = "dlt@crest.iu.edu"
REPORT_EMAIL           = "jemusser@umail.iu.edu" # email string
VALIDATION_GRANULARITY = 1024

##################################
#                                #
#       Auth Related settings    #
#                                #
##################################

AUTH_FIELD = "secToken"
AUTH_VALUE = ["landsat"]

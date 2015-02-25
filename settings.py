#**********************************************************
#                                                         #
#                       harvest.cfg                       #
#               config file for EODN_Harvest              #
#                                                         #
#**********************************************************
#
#
#**********************************************************
#                                                         #
#                       USGS M2M Info                     #
#                                                         #
#**********************************************************
# usgs_login: USGS login name
# usgs_passwd: USGS password
# These creditials are used to log into the USGS M2M server
# The USGS account and password must be autorized for M2M access
# If these values are None, you will be prompted to enter a
# user name and password at run time.

config = {
    "usgs_login": "prblackwell",
    "usgs_password": "g00d4USGS",

#  usgs_url: the USGS SOAP server WSDL URL
# use https for log in

    "usgs_url": "https://earthexplorer.usgs.gov/inventory/soap?wsdl",
# acceptable values:
#    https://lsiexplorer.cr.usgs.gov/inventory/soap?wsdl    CWIC/LSI Explorer
#    https://earthexplorer.usgs.gov/inventory/soap?wsdl     Earth Explorer
#    https://hddsexplorer.usgs.gov/inventory/soap?wsdl      HDDS Explorer
#    https://lpvexplorer.cr.usgs.gov/inventory/soap?wsdl    LPVS Explorer

# node: the USGS Node to access.  Acceptable values are:
#    CWIC	CWIC/LSI Explorer
#    EE		Earth Explorer
#    HDDS	HDDS Explorer
#    LPVS   LPVS Explorer

    "node": "EE",

# dataset: The data set name.  Acceptable values are:
#    Landsat_8	Landsat 8

    "data_set": "Landsat_8",

# data_type: The data type designation used for LoDN

    "data_type": "l8oli",

# ll: the lower left corner of the AOI bounding box in decimal degrees

    "ll": "-124.59,24.52",

# ur: the uppser right corner of the AOI bounding box in decimal degrees
    "ur": "-66.95,49.4",

# start: the starting date for the moving search window
# is start is None then start is calculated as Now - <days>

    "start": None,
#start 2014-10-17 20:40:42.706328

# days: the number of days for the search window.  
# Starting date is today - days.  Ending date is today

    "days": "1",

# max_recs: the number of records to return in each batch of search results
# The allowable range is between 1 and about 20.  More than that will
# time out

    "max_recs": "15",

# codes: product codes (downloadCodes) to look for 
# currently only accepts 'all'

    "codes": "all",

# cloud: the maximum allowable percentage of cloud cover
# *******   this isn't currently working  ***********
# This is used as an additionalCriterial argument for the search
# allowable value  name
#           None   'All'
#             0    'Less than 10%'
#             1    'Less than 20%'
#             2    'Less than 30%'
#             3    'Less than 40%'
#             4    'Less than 50%'
#             5    'Less than 60%'
#             6    'Less than 70%'
#             7    'Less than 80%'
#             8    'Less than 90%'
#             9    'Less than 100%'

    "cloud": "4",

# criteria: a list of additional criteria for the search
# {'keywork'='string_value', keyword'=int_value, ...

    "criteria": {'Cloud_Cover': 4 },


# sort: Sort order for search results

    "sort": "ASC",

# retry: The number of times to retry M2M functions
# Default = 10

    "retry": "10",

#**********************************************************
#                                                         #
#                         EODN Info                       #
#                                                         #
#**********************************************************
# lodn_url: the Ul of the LoDN server instance

    "lodn_url": "dlt.incntre.iu.edu:5000/",
#lodn_url tvdlnet0.sfasu.edu:5000/

# lodn_root: the LoDN root directory for this dataset

    "lodn_root": "eodn",

#**********************************************************
#                                                         #
#                  GloVis and AVMSS                       #
#                                                         #
#**********************************************************

# glovis: flag to control upload to GloVis
# glovis

    "glovis": False,

# glovis_url: the URL of a GloVis Server
# if this parameter is None, glovis_url will be populated
# set glovis_url None to disable

    "glovis_url": "tvdlnet4.sfasu.edu",

    # glovis_env: path to the glovis envirnoment script

    "glovis_env": "/store/eodn/env/glovis_env.sh",
    #glovis_env /store/glovis_8_20/env/glovis_env.sh

    # glovis_util: path to the download_usgs_scenes.pl script

    "glovis_util": "/store/eodn/coop_utils/download_usgs_scenes.pl",
    #glovis_util /store/glovis_8_20/coop_utils/download_usgs_scenes.pl

# avmss: flag to control upload to AVMSS
# avmss True send scene to AVMSS

    "avmss": False,

# avmss_url: url of the AVMSS server ingest script
# set avmss_url None to disable loading to AVMSS

    "avmss_url": "wms.americaview.org",
    #avmss_path /opt/eodn/incoming/
    "avmss_path": "/tmp/fr_bund/",
    "avmss_cmd": "/home/ssec/EODN/process_eodn.sh",

#**********************************************************
#                                                         #
#                      Workspace                          #
#                                                         #
#**********************************************************


# workspace: path to temporary working space
# default /tmp/

#workspace /Users/prb/Desktop/eodn_data/
#workspace /mnt/l8/l8oli/
#workspace /data/prb/eodn/
#workspace /data/prb/eodn/test/
    "workspace": "/data/jemusser/",

# pause: wait time between files in seconds

    "pause": "0",

#**********************************************************
#                                                         #
#                          Logs                           #
#                                                         #
#**********************************************************
# logging: logs all transactions
# log_file: path to the log file
    "default": True,

    "logging": True,
    "log_file": "/tmp/harvest.log",
# log_file /home/prb/eodn/logs/harvest.log

# error_logging logs errors
# error_path path to the errorlog
# default True

    "error_logging": True,
    "error_file": "/tmp/harvest_error.log",
# error_file /home/prb/eodn/logs/harvest_error.log

# download_logging: Logs download speeds 
# download_file: path to the download file

    "download_logging": True,
    "download_file": "/tmp/harvest_download.log",
# download_file /home/prb/eodn/logs/harvest_download.log

#**********************************************************
#                                                         #
#                  Test and Debug Flags                   #
#                                                         #
#**********************************************************
# The follow flags are used mostly for testing and debugging
# Most can be overwritten by command line options

# small_files: restricts max file size for faster testing
# default False

    "small_files": False,

# file_stat: test for existence of the file in the workspace
# default True

    "file_stat": True,

# file_download: flag to control downloading files from USGS
# setting download to False prevents files from being downloaded
# default True

    "file_download": True,

# file_delete: flag to control deleting files from local storage
# setting file_delete to True may result in a large accumulation of files
# default True

    "file_delete": False,

# lors_upload: upload files to EODN using LoRS
# Setting lors to False prevents uploads to EODN
# default True

    "lors_upload": True,

# lodn_stat: flag to control testing for existance of exNode on LoDN
# setting lodn_stat False forces LoDN import
# default True

    "lodn_stat": True ,

# lodn_import: import exnodes to EODN
# Setting lodn_import to False prevents populating exNodes to Lodn
# default True

    "lodn_import": False,

# unis_import: import exnodes to UNIS
# Setting lodn_import to False prevents populating exNodes to UNIS
# default True

    "unis_import": True,
    }

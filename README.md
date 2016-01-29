
# About

EODNHarvester is a daemon that gathers scenes from the United States Geological Survey (USGS) using the USGS JSON interface.  The USGS hosts satellite imagery collected by a number of sensors.  These images (a collection of images of a single location - e.g. Standard and Infared - is called a scene) are uploaded to the Earth Observation Depot Network (EODN) for use and data collection.

# Installation

EODNHarvester is a python script and can be installed using the `setup.py` script.  This requires Python3.x.

## Prerequisites

Ensure your system has python3.x installed.

    python3 -V

If python3 is not installed, the process will depend on your OS.

NOTE: Python3 must be installed as a alternate version of Python on RedHat (and RedHat derived distributions) as yum requires Python2 to be the primary python version.

## EODNHarvester

EODNHarvester can be installed simply by calling the `setup.py` script.

    ./setup.py build

Then again to install:

    ./setup.py install

If necessary, a prefix can be provided to install to an explicit bin folder.  The following will install eodnharvester to the /usr/local/bin folder:

    ./setup.py install --prefix=/usr/local


# Usage

EODNHarvester is designed to be a mostly self sufficient daemon, and can be run simply by calling:

    eodnharvesterd

If this does not work, ensure that the EODNHarvester has been installed to a directory in your path.  For debugging purposes, two flags are available:

    eodnharvesterd -v

and

    eodnharvesterd -D

The former will output more verbose messages, while the latter will output more robust debugging logging.

By default, the EODNHarvester runs as a standard script, by passing the `-d` flag, it will run as a daemon.

    eodnharvesterd -d

To set up a new configuration for EODNHarvester, run with the -c flag and follow the instructions in the interface.

    eodnharvesterd -c


## Settings

EODNHarvester also includes a large number of settings in the `eodnharvester/settings.py' file.  These settings will not take effect unless the project is re-built and re-installed.

    VERBOSE        | This is an override for the -v flag.
                   | If set to true will always emit verbose messages.
    DEBUG          | Same as above for the -D flag.
    THREADS        | Sets the number of threads for the script to use.
    MAX_RECONNECT  | The number of times the EODNHarvester will attempt
                   | to contact USGS before sleeping.
    HARVEST_WINDOW | How often the EODNHarvester will collect scenes.
    WORKSPACE      | The directory to store the files currently being harvested.
                   | These files are temporary and removed on harvest completion.
    USGS_HOST      | The url used to contact the USGS.
    USERNAME       | The username used to log in to USGS.
    PASSWORD       | The password used to log in to USGS.
    TIMEOUT        | The amount of time to wait before retrying network connections.
    DATASET_NAME   | The USGS satellite being collected from.
    LOWER_LEFT     | The latitude and longitude coordinates of the lower left
                   | corner of the harvested area.
    UPPER_RIGHT    | The latitude and longitude coordinates of the upper right
                   | corner of the harvested area.
    MAX_RESULTS    | The number of scenes harvested in a single pass
                   | (Timeouts occur more often above 15 results)
    SORT_ORDER     | The order to recieve results from USGS in.
    NODE           | The type of data being recieved from USGS.
    UNIS_HOST      | The url or ip of the UNIS instance used to store exnode information.
    UNIS_PORT      | The port number of the UNIS instance used to store exnode information.
    LoRS           | Settings for the LoRS upload. (Read the LoRS documentation
                   | for more info on these settings).
    REPORT_HOUR    | What hour of the day the periodic report should be sent out.
    REPORT_EMAIL   | The email to send reports to.
    AUTH_FIELD     | Where to store security information in the exnode.
    AUTH_VALUE     | The token used for the exnode.

    VALIDATION_GRANULARITY | Size of chunks to be used when checking
                           | files for correctness.
    DOWNLOAD_CHUNKSIZE     | Chunk size to use when downloading data from USGS.
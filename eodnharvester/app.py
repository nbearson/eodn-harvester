#!/usr/bin/env python3
#############################################
#  EODNHarvest gathers landsat imagery from #
#  the USGS and exports it to EODN.         #
#                                           #
#  @author:  Jeremy Musser                  #
#  @date:    06/07/2015                     #
#  @version: 0.3.1                          #
#############################################
import daemon
import datetime
import time
import requests
import argparse
import sys
import os
import subprocess
import concurrent.futures
import json

import eodnharvester.history as history
import eodnharvester.settings as settings
import eodnharvester.reporter as reporter
from eodnharvester.search import Search


window_start = datetime.datetime.utcnow() - datetime.timedelta(**settings.HARVEST_WINDOW)
window_end = datetime.datetime.utcnow()
AUTH_FIELD = settings.AUTH_FIELD
AUTH_VALUE = settings.AUTH_VALUE

def productExists(product):
    logger = history.GetLogger()
    url = "http://{host}:{port}/exnodes?metadata.scene={scene}&metadata.productCode={code}".format(host  = settings.UNIS_HOST,
                                                                                                   port  = settings.UNIS_PORT,
                                                                                                   scene = product.scene,
                                                                                                   code  = product.productCode)

    try:
        response = requests.get(url)
        response = response.json()
    except requests.exceptions.RequestException as exp:
        error = "Failed to connect to UNIS - {exp}".format(exp = exp)
        logger.error(error)
        return False
    except ValueError as exp:
        error = "Error while decoding unis json - {exp}".format(exp = exp)
        logger.error(error)
        return False
    except Exception as exp:
        error = "Unkown error while contacting UNIS - {exp}".format(exp = exp)
        logger.error(error)
        return False
    
    if response:
        return True
    else:
        return False


def _getUnisDirectory(basename):
    directory = {
        "sensor": basename[:3],
        "path":   basename[3:6],
        "row":    basename[6:9],
        "year":   basename[9:13]
    }
    
    return "/Landsat/{sensor}/{path}/{row}/{year}".format(**directory)


def downloadProduct(product, log = None):
    logger = history.GetLogger()
    if not log:
        log = Report()

    logger.info("Downloading {name} from USGS".format(name = product.filename))

    output_file = "{workspace}/{filename}".format(workspace = settings.WORKSPACE, 
                                                  filename = product.filename)
    filesize = 0
    start_time = datetime.datetime.utcnow()
    
    try:
        response = requests.get(product.downloadUrl, stream = True, timeout = settings.TIMEOUT)
    except requests.exceptions.RequestException as exp:
        error = "Failed to connect to download service - {exp}".format(exp = exp)
        logger.error(error)
        log.error(history.SYS, error)
        return False
    except Exception as exp:
        error = "Unknown error while downloading file - {exp}".format(exp = exp)
        logger.error(error)
        log.error(history.SYS, error)
        return False

    try:
        with open(output_file, 'wb') as f:
            if settings.VERBOSE and settings.THREADS <= 1:
                sys.stdout.write("\n")
            
            for chunk in response.iter_content(chunk_size = settings.DOWNLOAD_CHUNKSIZE):
                if not chunk:
                    continue
                
                f.write(chunk)
                f.flush()
                filesize += len(chunk)
                
                if settings.VERBOSE and settings.THREADS <= 1:
                    percent = float(filesize) / float(product.filesize)
                    sys.stdout.write("\r[{bar:<30}] {percent:0.2f}%  <{so_far:>10} of {total:<10}>".format(bar     = "#" * int(30 * percent), 
                                                                                                           percent = float(percent * 100),
                                                                                                           so_far  = filesize,
                                                                                                           total   = product.filesize))
                    sys.stdout.flush()
    except Exception as exp:
        error = "Unknown error while opening and storing file - {exp}".format(exp = exp)
        logger.error(error)
        log.error(history.SYS, error)
    finally:
        if settings.VERBOSE and settings.THREADS <= 1:
            sys.stdout.write("\n")

    end_time = datetime.datetime.utcnow()
    
    try:
        delta = end_time - start_time
        delta_s = (delta.days * 3600 * 24) + delta.seconds
        delta_micro = (delta_s * 10**6) + delta.microseconds
        speed = float(filesize) / float(delta_micro)
        log.write(product.filename, "filesize", str(filesize))
        log.write(product.filename, "download_speed", "{speed:0.3f} bytes/s".format(speed = speed * 10**6))
    except Exception as exp:
        logger.error("Unable to calculate download speed")

    return output_file
    

def lorsUpload(filename, basename):
    result = '0'
    output = "http://{unis_host}:{unis_port}/exnodes".format(unis_host = settings.UNIS_HOST,
                                                             unis_port = settings.UNIS_PORT)
    logger = history.GetLogger()
    directory = _getUnisDirectory(basename)

    try:
        duration = "--duration={0}h".format(settings.LoRS["duration"])
        call = subprocess.Popen(['lors_upload', duration,
                                 '--none',
                                 '-c', str(settings.LoRS["copies"]),
                                 '-m', str(settings.LoRS["depots"]),
                                 '-t', str(settings.LoRS["threads"]),
                                 '-b', str(settings.LoRS["size"]),
                                 '--depot-list',
                                 '--xndrc={xndrc}'.format(xndrc = settings.LoRS["xndrc"]),
                                 '-V', '1' if settings.VERBOSE else '0',
                                 '-u', directory,
                                 '-o', output, filename], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        
        out, err = call.communicate()
        result   = call.returncode
        if settings.VERBOSE:
            if out:
                logger.info(out.decode('utf-8'))
            if err:
                logger.error(err.decode('utf-8'))
 
    except Exception as exp:
        logger.error("Unknown error while calling lors_upload - {exp}".format(exp = exp))
            
    return result

    
def addMetadata(product):
    logger = history.GetLogger()
    url = "http://{host}:{port}/exnodes?name={name}".format(host = settings.UNIS_HOST,
                                                     port = settings.UNIS_PORT,
                                                     name = product.filename)
    try:
        response = requests.get(url)
        response = response.json()[0]
        tmpId    = response["id"]
        
        if "metadata" not in response:
            response["metadata"] = {}

        response["metadata"]["productCode"] = product.productCode
        response["metadata"]["scene"] = product.scene
        response[AUTH_FIELD] = AUTH_VALUE
        
        url = "http://{host}:{port}/exnodes/{uid}".format(host = settings.UNIS_HOST,
                                                          port = settings.UNIS_PORT,
                                                          uid  = tmpId)
        response = requests.put(url, data = json.dumps(response))
        response = response.json()
    except requests.exceptions.RequestException as exp:
        error = "Failed to connect to UNIS - {exp}".format(exp = exp)
        logger.error(error)
        return False
    except ValueError as exp:
        error = "Error while decoding unis json - {exp}".format(exp = exp)
        logger.error(error)
        return False
    except Exception as exp:
        error = "Unkown error while contacting UNIS - {exp}".format(exp = exp)
        logger.error(error)
        return False

    return True


def createProduct(product):
    log = history.Record()
    logger = history.GetLogger()
    
    if productExists(product):
        logger.info("Product on record, skipping...")
        return log

    if product.initialize():
        filename = downloadProduct(product, log)
        if not filename:
            return log
    else:
        return log
    
    lorsUpload(filename, product.basename)

    if not addMetadata(product):
        error = "LoRS upload failed - {errno}".format(errno = errno)
        logger.info(error)
        log.error(product.filename, error)
        
    try:
        logger.info("Removing {product}".format(product = product.filename))
        os.remove(filename)
    except Exception as exp:
        error = "Failed to remove local file - {exp}".format(exp = exp)
        logger.error(error)
        log.error(product.filename, error)
        return log

    if str(errno) == '0':
        log.write(product.filename, "complete", True)
    return log



def harvest(scene):
    log = history.Record()
    logger = history.GetLogger()
    logger.info("Starting work on {scene_id}".format(scene_id = scene.entity_id))
    
    if settings.THREADS > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
            for report in executor.map(createProduct, scene):
                log.merge(report)
    else:
        for product in scene:
            report = createProduct(product)
            log.merge(report)

    return log




def createSearchParams():
    global window_start
    global window_end
    result = {}
    result["datasetName"] = settings.DATASET_NAME
    result["lowerLeft"]   = settings.LOWER_LEFT
    result["upperRight"]  = settings.UPPER_RIGHT
    result["startDate"]   = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    result["endDate"]     = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    result["maxResults"]  = settings.MAX_RESULTS
    result["sortOrder"]   = settings.SORT_ORDER
    result["node"]        = settings.NODE
    
    return result

def run():
    global window_start
    global window_end
    logger = history.GetLogger()
    logger.info("Starting harvester....")
    log = history.Record()
    
    while True:
        try:
            window_end = datetime.datetime.utcnow()
            new_start = window_end
            search = Search(**createSearchParams())
            
            if settings.THREADS > 1:
                with concurrent.futures.ThreadPoolExecutor(max_workers = settings.THREADS) as executor:
                    for report in executor.map(harvest, search):
                        log.merge(report)
            else:
                for scene in search:
                    report = harvest(scene)
                    log.merge(report)
                            
            log.merge(search.log)
            reporter.CreateReport(log)

            window_start = new_start
            delay_time = datetime.datetime.utcnow() - new_start
            if delay_time < datetime.timedelta(**settings.HARVEST_WINDOW):
                remaining_time = datetime.timedelta(**settings.HARVEST_WINDOW) - delay_time
                remaining_seconds = remaining_time.seconds + (remaining_time.days * 24 * 60 * 60)
                logger.info("--Sleeping for {s} seconds...".format(s = remaining_seconds))
                time.sleep(remaining_seconds)
        except Exception as exp:
            logger.info("Critical failure: {exp} - Restarting harvest".format(exp = exp))
    
        

def main():

    parser = argparse.ArgumentParser(description = "Harvest data for EODN")
    parser.add_argument('-v', '--verbose', action = 'store_true', help = "Makes the output verbose")
    parser.add_argument('-D', '--debug', action = 'store_true', help = "Includes debugging messages in output")
    parser.add_argument('-d', '--daemon', action = 'store_true', help = "Indicates that the process should be run as a daemon")
    args = parser.parse_args()

    if args.verbose:
        settings.VERBOSE = True
    
    if args.debug:
        settings.DEBUG = True

    if args.daemon:
        with daemon.DaemonContext():
            run()
    else:
        run()


if __name__ == "__main__":
    main()
    

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
import eodnharvester.auth as auth
from eodnharvester.search import Search
from eodnharvester.conf import HarvesterConfigure



AUTH_FIELD = settings.AUTH_FIELD
AUTH_VALUE = settings.AUTH_VALUE

def productExists(product):
    logger = history.GetLogger()
    url = "{protocol}://{host}:{port}/exnodes?metadata.scene={scene}&metadata.productCode={code}".format(protocol = "https" if settings.USE_SSL else "http",
                                                                                                         host  = settings.UNIS_HOST,
                                                                                                         port  = settings.UNIS_PORT,
                                                                                                         scene = product.scene,
                                                                                                         code  = product.productCode)

    try:
        response = requests.get(url, cert = (settings.SSL_OPTIONS["cert"], settings.SSL_OPTIONS["key"]))
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


def downloadProduct(product):
    logger = history.GetLogger()
    log = history.Record()

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
        return False, log
    except Exception as exp:
        error = "Unknown error while downloading file - {exp}".format(exp = exp)
        logger.error(error)
        log.error(history.SYS, error)
        return False, log

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

    return output_file, log
    

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
    url = "{protocol}://{host}:{port}/exnodes?name={name}".format(protocol = "https" if settings.USE_SSL else "http",
                                                                  host = settings.UNIS_HOST,
                                                                  port = settings.UNIS_PORT,
                                                                  name = product.filename)
    try:
        response = requests.get(url, cert = (settings.SSL_OPTIONS["cert"], settings.SSL_OPTIONS["key"]))
        response = response.json()[0]
        tmpId    = response["id"]
        
        if "metadata" not in response:
            response["metadata"] = {}

        response["metadata"]["productCode"] = product.productCode
        response["metadata"]["scene"] = product.scene
        response[AUTH_FIELD] = AUTH_VALUE
        
        url = "{protocol}://{host}:{port}/exnodes/{uid}".format(protocol = "https" if settings.USE_SSL else "http",
                                                                host = settings.UNIS_HOST,
                                                                port = settings.UNIS_PORT,
                                                                uid  = tmpId)
        response = requests.put(url, data = json.dumps(response), cert = (settings.SSL_OPTIONS["cert"], settings.SSL_OPTIONS["key"]))
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
    logger = history.GetLogger()
    
    if productExists(product):
        logger.info("Product on record, skipping...")
        return None

    if product.initialize():
        filename, log = downloadProduct(product)
        if not filename:
            return log
    else:
        return None
    
    lorsUpload(filename, product.basename)
    
    if addMetadata(product):
        log.write(product.filename, "complete", True)
        
    try:
        logger.info("Removing {product}".format(product = product.filename))
        os.remove(filename)
    except Exception as exp:
        error = "Failed to remove local file - {exp}".format(exp = exp)
        logger.error(error)
        log.error(product.filename, error)
        
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


def run():
    logger = history.GetLogger()
    logger.info("Starting harvester for {name}....".format(name = settings.HARVEST_NAME))
    log = history.Record()
    
    window_start = datetime.datetime.utcnow() - datetime.timedelta(**settings.HARVEST_WINDOW)
    window_end = datetime.datetime.utcnow()
    configManager = HarvesterConfigure()
    
    while True:
        try:
            window_end = datetime.datetime.utcnow()
            new_start = window_end
            for config in configManager.get():
                config["startDate"] = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                config["endDate"]   = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                config["maxResults"]  = settings.MAX_RESULTS
                
                search = Search(**config)
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
                auth.logout(log, force = True)
                remaining_time = datetime.timedelta(**settings.HARVEST_WINDOW) - delay_time
                remaining_seconds = remaining_time.seconds + (remaining_time.days * 24 * 60 * 60)
                logger.info("--Sleeping for {s} seconds...".format(s = remaining_seconds))
                time.sleep(remaining_seconds)
                
        except Exception as exp:
            logger.info("Critical failure: {exp} - Restarting harvest".format(exp = exp))
    
        

def config():
    configManager = HarvesterConfigure()
    
    def cinput(prompt, cls = None, use_exit = True, default = ""):
        invalid = True
        
        while True:
            try:
                val = raw_input(prompt)
            except Exception as e:
                val = input(prompt)
            if val == "exit" and use_exit:
                return None
            try:
                if not val:
                    if default:
                        return default
                    else:
                        raise Exception("No value given")
                if type(cls) is list:
                    if str(val) not in cls:
                        raise Exception("Value not in list")
                elif cls == "float":
                    val = float(val)
                elif cls == "int":
                    val = int(val)
                elif cls == "str":
                    val = str(val)
            except Exception as exp:
                print("Invalid input, please check your entry")
                continue
            break
        return val
    
    def get_action():
        return cinput("Choose an action [ list | add | edit | remove | exit ]: ", ["list", "add", "edit", "remove", "exit"], False)
    
    def list_configs():
        configs = configManager.get()
        if not configs:
            return None
        
        for index in range(len(configs)):
            print("\n[{index}]".format(index = index + 1))
            print(json.dumps(configs[index], indent = 2))
            
        return configs
    
    def choose_config():
        while True:
            configs = list_configs()
            if not configs:
                return None
            
            val = cinput("Please choose a configuration to modify [1-{index}]: ".format(index = len(configs)), "int")
            if not val:
                return None
            
            val = val - 1
            if val < len(configs) and val >= 0:
                return val
            
            
    def create_config():
        print("Please enter the configuration values")
        dataset = cinput("Dataset Name [Landsat_8]: ", "str", default = "Landsat_8")
        if not dataset:
            return None
        
        LLlat   = cinput("Lower left Latitude: ", "float")
        if not LLlat:
            return None
        
        LLlon = cinput("Lower left Longitude: ", "float")
        if not LLlon:
            return None
        
        URlat = cinput("Upper right Latitude: ", "float")
        if not URlat:
            return None
        
        URlon = cinput("Upper right Longitude: ", "float")
        if not URlon:
            return None

        sort = cinput("Sort order [ASC|DESC]: ", ["ASC", "DESC"])
        if not sort:
            return None
        
        node = cinput("Node name [EE]: ", "str", default = "EE")
        if not node:
            return None
        
        config = {}
        config["datasetName"] = dataset
        if not config["datasetName"]:
            config["datasetName"] = "Landsat_8"
        config["lowerLeft"] = {}
        config["lowerLeft"]["latitude"] = LLlat
        config["lowerLeft"]["longitude"] = LLlon
        config["upperRight"] = {}
        config["upperRight"]["latitude"] = URlat
        config["upperRight"]["longitude"] = URlon
        config["sortOrder"] = sort
        config["node"] = node
        if not config["node"]:
            config["node"] = "EE"
            
        return config
    
    action = ""
    while action != "exit":
        action = get_action()
        if action == "list":
            list_configs()
        if action == "add":
            config = create_config()
            if config:
                configManager.add(config)
        elif action == "edit":
            cid = choose_config()
            if cid is not None:
                config = create_config()
                if config:
                    configManager.edit(cid, config)
        elif action == "remove":
            cid = choose_config()
            if cid is not None:
                configManager.remove(cid)

            
def main():
    parser = argparse.ArgumentParser(description = "Harvest data for EODN")
    parser.add_argument('-c', '--configure', action = 'store_true', help = "Run in configuration mode")
    parser.add_argument('-v', '--verbose', action = 'store_true', help = "Makes the output verbose")
    parser.add_argument('-D', '--debug', action = 'store_true', help = "Includes debugging messages in output")
    parser.add_argument('-d', '--daemon', action = 'store_true', help = "Indicates that the process should be run as a daemon")
    args = parser.parse_args()
    
    if not os.path.exists(settings.WORKSPACE):
        os.makedirs(settings.WORKSPACE)

    if not os.path.exists("{ws}/log".format(ws = settings.WORKSPACE)):
        os.makedirs("{ws}/log".format(ws = settings.WORKSPACE))
    
    if args.verbose:
        settings.VERBOSE = True
    
    if args.debug:
        settings.DEBUG = True

    if args.configure:
        config()
    elif args.daemon:
        with daemon.DaemonContext():
            run()
    else:
        run()


if __name__ == "__main__":
    main()
    

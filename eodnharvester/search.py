#!/usr/bin/python

################################################################
# search.py                                                    #
#           Contains classes used when searching USGS entities #
#                                                              #
# @author:  Jeremy Musser                                      #
# @author:  Paul Blackwell                                     #
################################################################

import json
import concurrent.futures
import requests

from concurrent.futures import ThreadPoolExecutor

import eodnharvester.history as history
import eodnharvester.settings as settings
import eodnharvester.auth as auth

from eodnharvester.entity import Entity

class Search(object):
    def __init__(self, **kwargs):
        self.log = history.Record()
        self.entities = []
        
        self.search = {}
        self.search["datasetName"] = kwargs.get("datasetName", None)
        self.search["lowerLeft"]   = kwargs.get("lowerLeft", None)
        self.search["upperRight"]  = kwargs.get("upperRight", None)
        self.search["startDate"]   = kwargs.get("startDate", None)
        self.search["endDate"]     = kwargs.get("endDate", None)
        self.search["node"]        = kwargs.get("node", "EE")
        self.search["sortOrder"]   = kwargs.get("sortOrder", "ASC")
        self.search["maxResults"]  = kwargs.get("maxResults", 10)
        
    
    def find(self, startingNumber = 1):
        logger = history.GetLogger()
        apiKey = auth.login(self.log)

        self.search["startingNumber"] = startingNumber
        self.search["apiKey"] = apiKey

        response = ""
        url = "http://{usgs_host}/inventory/json/{request_code}".format(usgs_host    = settings.USGS_HOST,
                                                                        request_code = "search")
        
        logger.info("Searching USGS for scenes...")
        
        
        try:
            logger.info("{key:>15}: {value}".format(key = "Dataset", value = self.search["datasetName"]))
            logger.info("{key:>15}: {value}".format(key = "Lower Left", value = self.search["lowerLeft"]))
            logger.info("{key:>15}: {value}".format(key = "Upper Right", value = self.search["upperRight"]))
            logger.info("{key:>15}: {value}".format(key = "Start Date", value = self.search["startDate"]))
            logger.info("{key:>15}: {value}".format(key = "End Date", value = self.search["endDate"]))
            logger.info("{key:>15}: {value}".format(key = "Node", value = self.search["node"]))
            logger.debug("{url}?jsonRequest={params}".format(url = url, params = json.dumps(self.search)))
            response = requests.get(url, params = { 'jsonRequest': json.dumps(self.search) }, timeout = settings.TIMEOUT)
            response = response.json()
            logger.debug(response)
        except requests.exceptions.RequestException as exp:
            error = "Failed to get scene data - {exp}".format(exp = exp)
            logger.error(error)
            self.log.error(history.SYS, error)
            auth.logout(self.log)
            raise Exception(error)
        except ValueError as exp:
            error = "Error while decoding scene json - {exp}".format(exp = exp)
            logger.error(error)
            self.log.error(history.SYS, error)
            auth.logout(self.log)
            return 0
        except Exception as exp:
            error = "Unknown error while getting scene data - {exp}".format(exp = exp)
            logger.error(error)
            self.log.error(history.SYS, error)
            auth.logout(self.log)
            return 0
            
        if response["errorCode"]:
            error = "Error from USGS - {err}".format(err = response["error"])
            logger.error(error)
            self.log.error(history.SYS, error)
            auth.logout(self.log)
            return 0
        
        logger.info("Completed search of USGS:")
        logger.info("Recieved {numberReturned} of {totalHits}".format(numberReturned = response["data"]["numberReturned"],
                                                                       totalHits      = response["data"]["totalHits"]))
        logger.info("         Processing Scenes {firstRecord} to {lastRecord}".format(firstRecord = response["data"]["firstRecord"],
                                                                                       lastRecord  = response["data"]["lastRecord"]))
        
        for entity in response["data"]["results"]:
            entity["datasetName"] = self.search["datasetName"]
            entity["node"]        = self.search["node"]
            tmpEntity = Entity(**entity)
            self.entities.append(tmpEntity)
            
        auth.logout(self.log)
        return response["data"]["numberReturned"]
        
    
    def __iter__(self):
        return _search_iter(self)
    

class _search_iter(object):
    def __init__(self, search):
        self.index      = 0
        self.lastNumber = 0
        self.search     = search

    def __iter__(self):
        return self

    # Python2.x backwards compatibility
    def next(self):
        return self.__next__()

    def __next__(self):
        if self.index == self.lastNumber:
            self.lastNumber += self.search.find(self.index + 1)
        
        try:
            entity = self.search.entities[self.index]
            self.index += 1
        except Exception as exp:
            raise StopIteration()
        
        return entity
            

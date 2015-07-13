#!/usr/bin/python

###################################################
# entitiy.py                                      #
#            Explicit class for entity objects    #
#                                                 #
# @author: Jeremy Musser                          #
###################################################

import requests
import json

import history
import settings
import auth

from product import Product

class Entity(object):
    def __init__(self, **kwargs):
        self.entity_id = kwargs.get("entityId", None)

        self.populated = False
        self.log = history.Record()

        self.metadata = {}
        self.products = []

        self.metadata["acquisitionDate"] = kwargs.get("acquisitionDate", None)
        self.metadata["startTime"]       = kwargs.get("startTime", None)
        self.metadata["endTime"]         = kwargs.get("endTime", None)
        self.metadata["browseUrl"]       = kwargs.get("browseUrl", None)
        self.metadata["downloadUrl"]     = kwargs.get("downloadUrl", None)
        self.metadata["lowerLeftCoordinate"]  = kwargs.get("lowerLeftCoordinate", None)
        self.metadata["upperRightCoordinate"] = kwargs.get("upperRightCoordinate", None)
        self.metadata["upperLeftCoordinate"]  = kwargs.get("upperLeftCoordinate", None)
        self.metadata["lowerRightCoordinate"] = kwargs.get("lowerRightCoordinate", None)


    def GetProducts(self):
        logger = history.GetLogger("harvest")
        apiKey = auth.login(self.log)

        self.products = []
        response = ""
        url = "http://{usgs_host}/inventory/json/{request_code}"

        sceneRequest = {
            "datasetName": settings.DATASET_NAME,
            "apiKey":      apiKey,
            "node":        settings.NODE,
            "entityIds":   [self.entity_id]
        }
        scene_url = url.format(usgs_host    = settings.USGS_HOST,
                               request_code = "downloadoptions")

        try:
            logger.info("Getting information on {product} products".format(product = self.entity_id))
            logger.debug("{url}?jsonRequest={params}".format(url = scene_url, params = json.dumps(sceneRequest)))
            response = requests.get(scene_url, params = { 'jsonRequest': json.dumps(sceneRequest) }, timeout = settings.TIMEOUT)
            response = response.json()
            logger.debug(response)
        except requests.exceptions.RequestException as exp:
            error = "Failed to get scene metadata - {exp}".format(exp = exp)
            logger.error(error)
            self.log.error(self.entity_id, error)
            auth.logout(self.log)
            return False
        except ValueError as exp:
            error = "Error while decoding scene json - {exp}".format(exp = exp)
            logger.error(error)
            self.log.error(self.entity_id, error)
            auth.logout(self.log)
            return False
        except Exception as exp:
            error = "Unknown error while getting scene metadata - {exp}".format(exp = exp)
            logger.error(error)
            self.log.error(self.entity_id, error)
            auth.logout(self.log)
            return False


        for entity in response["data"][0]["downloadOptions"]:
            product = Product(self.entity_id, entity["downloadCode"])
            self.products.append(product)
        
        auth.logout(self.log)
        self.populated = True
        return True


    def __iter__(self):
        return _entity_iter(self)




class _entity_iter(object):
    def __init__(self, entity):
        self.entity = entity
        self.index = 0

    def __iter__(self):
        return self

    # Python2.x backwards compatibility
    def next(self):
        return self.__next__()

    def __next__(self):
        try:
            if not self.entity.populated:
                self.entity.GetProducts()
            
            product = self.entity.products[self.index]
            self.index += 1
        except Exception as exp:
            raise StopIteration()

        return product

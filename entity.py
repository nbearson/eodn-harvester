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
            logger.info("Getting download url for {product}".format(product = entity["downloadCode"]))
            downloadRequest = {
                "datasetName": settings.DATASET_NAME,
                "apiKey":      apiKey,
                "node":        settings.NODE,
                "entityIds":   [self.entity_id],
                "products":    [entity["downloadCode"]]
            }
            download_url = url.format(usgs_host    = settings.USGS_HOST,
                                      request_code = "download")

            try:
                logger.debug("{url}?jsonRequest={params}".format(url = download_url, params = json.dumps(downloadRequest)))
                entity_data = requests.get(download_url, params = { 'jsonRequest': json.dumps(downloadRequest) }, timeout = settings.TIMEOUT)
                entity_data = entity_data.json()
                logger.debug(entity_data)
                
                if entity_data["errorCode"]:
                    error = "Recieved error from USGS - {err}".format(err = entity_data["error"])
                    logger.error(error)
                    self.log.error(self.entity_id, error)
                    continue
            except requests.exceptions.RequestException as exp:
                logger.error("Failed to get entity metadata - {exp}".format(exp = exp))
                error = {}
                error[entity["downloadCode"]] = exp
                self.log.error(self.entity_id, error)
                continue
            except ValueError as exp:
                logger.error("Error while decoding entity json - {exp}".format(exp = exp))
                error = {}
                error[entity["downloadCode"]] = exp
                self.log.error(self.entity_id, error)
                continue
            except Exception as exp:
                logger.error("Unknown error while getting entity metadata - {exp}".format(exp = exp))
                error = {}
                error[entity["downloadCode"]] = exp
                self.log.error(self.entity_id, error)
                continue
            
            try:
                if len(entity_data["data"]) == 0:
                    raise Exception("No download URL recieved")
                tmpURL = entity_data["data"][0]
                basename = self._getBasename(tmpURL)
                filename = self._getFilename(basename, entity["downloadCode"])

                product_log = history.GetLogger(basename)
                product_log.info("  Found {product} at {url}".format(product = entity["downloadCode"], url = tmpURL))
                product = {
                    "basename":    basename,
                    "filename":    filename,
                    "filesize":    entity["filesize"],
                    "downloadUrl": tmpURL,
                    "metadata":    self.metadata
                }
                self.products.append(product)
            except Exception as exp:
                logger.error("Recieved bad download data from USGS - {exp}".format(exp = exp))
        
        auth.logout(self.log)
        self.populated = True
        return True


    def _getBasename(self, url):
        parts = url.rsplit('/')
        body  = parts[-1]
        basename = body.split('?')[0]
        return basename

    def _getFilename(self, basename, download_code):
        if download_code == 'FR_REFL':
            return basename + ".jpg"
        elif download_code == 'FR_THERM':
            return basename + ".jpg"
        elif download_code == 'FR_QB':
            return basename + ".png"
        elif download_code == 'FR_BUND':
            return basename + ".zip"
        else:
            return basename
    

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

#!/usr/bin/python

###########################################
# product.py                              #
#             Explicit class for products #
#                                         #
# @author: Jeremy Musser                  #
###########################################

import requests
import json

import eodnharvester.history as history
import eodnharvester.auth as auth
import eodnharvester.settings as settings

class Product(object):
    def __init__(self, scene, productCode):
        self.productCode = productCode
        self.scene = scene

    def initialize(self):
        url = "http://{usgs_host}/inventory/json/{request_code}"
        logger = history.GetLogger()
        apiKey = auth.login()
        
        logger.info("Getting download url for {product}".format(product = self.productCode))
        downloadRequest = {
            "datasetName": settings.DATASET_NAME,
            "apiKey":      apiKey,
            "node":        settings.NODE,
            "entityIds":   [self.scene],
            "products":    [self.productCode]
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
                auth.logout()
                return False
        except requests.exceptions.RequestException as exp:
            logger.error("Failed to get entity metadata - {exp}".format(exp = exp))
            auth.logout()
            return False
        except ValueError as exp:
            logger.error("Error while decoding entity json - {exp}".format(exp = exp))
            auth.logout()
            return False
        except Exception as exp:
            logger.error("Unknown error while getting entity metadata - {exp}".format(exp = exp))
            auth.logout()
            return False
        
        try:
            if len(entity_data["data"]) == 0:
                raise Exception("No download URL recieved")
            tmpURL = entity_data["data"][0]
            self.basename    = self._getBasename(tmpURL)
            self.filename    = self._getFilename(self.basename, self.productCode)
            self.downloadUrl = tmpURL
            
            logger.info("  Found {product} at {url}".format(product = self.productCode, url = tmpURL))
        except Exception as exp:
            logger.error("Recieved bad download data from USGS - {exp}".format(exp = exp))
            auth.logout()
            return False
        
        auth.logout()
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
    


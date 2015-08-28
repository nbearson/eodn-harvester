
import logging
import os
import json
import datetime

from logging.handlers import RotatingFileHandler

import eodnharvester.settings as settings


SYS = "System"

def GetLogger():
    logger = logging.getLogger("harvest")
    
    if settings.DEBUG:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s-%(levelname)s| %(message)s", "%Y-%m-%d %H:%M:%S")

    handler = RotatingFileHandler("{workspace}/log/harvest.log".format(workspace = settings.WORKSPACE), maxBytes = 500000, backupCount = 5)
    handler.setLevel(logging.WARNING)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    if settings.VERBOSE:
        stdout = logging.StreamHandler()
        stdout.setLevel(logging.DEBUG)
        stdout.setFormatter(formatter)
        logger.addHandler(stdout)    
        
    return logger


class Record(object):
    _record = {}

    def __init__(self):
        self._record = {}

    def recordComplete(self, product_id):
        return product_id in self._record and "complete" in self._record[product_id]
    
    def hasRecord(self, product_id):
        return product_id in self._record
            

    def write(self, product_id, key, value):
        try:
            self._record[product_id][key] = value
        except Exception as exp:
            self._record[product_id] = {}
            self._record[product_id]["ts"] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            self._record[product_id][key] = value
            
    def error(self, product_id, value):
        ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        report = {}
        report[ts] = value
        try:
            self._record[product_id]["errors"].append(report)
        except Exception as exp:
            if product_id not in self._record:
                self._record[product_id] = {}

            self._record[product_id]["errors"] = []
            self._record[product_id]["errors"].append(report)

    def merge(self, log):
        if type(log) is not Record:
            return
    
        for product in log._record:
            if product not in self._record:
                self._record[product] = {}

            for key in log._record[product]:
                if key not in self._record[product]:
                    self._record[product][key] = []
                
                if key == "errors":
                    self._record[product][key] += log._record[product][key]
                else:
                    self._record[product][key] = log._record[product][key]

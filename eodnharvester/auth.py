

import requests
import time
import json
import datetime

import eodnharvester.history as history
import eodnharvester.settings as settings



_apiKey = ""
_aquired = None


def login(log = None):
    global _apiKey
    global _aquired
    if _apiKey and _aquired + datetime.timedelta(hours = 1) > datetime.datetime.utcnow():
        return _apiKey
    
    logger = history.GetLogger()
    if not log:
        log = history.Record()

    response = ""
    url = "https://{usgs_host}/inventory/json/{request_code}".format(usgs_host = settings.USGS_HOST,
                                                           request_code = "login")
    retry = 1
    
    logger.info("Logging into USGS")
    
    while retry:
        try:
            response = requests.post(url, data = { 'jsonRequest': json.dumps({ "username": settings.USERNAME,
                                                                               "password": settings.PASSWORD }) },
                                     timeout = settings.TIMEOUT)
            response = response.json()
            
            if response["errorCode"]:
                error = "Error from USGS while logging in - [{code}]{err}".format(code = response["errorCode"], err = response["error"])
                logger.error(error)
                log.error(history.SYS, error)
            else:
                break
        except requests.exceptions.RequestException as exp:
            error = "Failed to contact USGS - {exp}".format(exp = exp)
            logger.error(error)
            log.error(history.SYS, error)
        except ValueError as exp:
            error = "Error while decoding response - {exp} - {response}".format(exp = exp, response = response.text)
            logger.error(error)
            log.error(history.SYS, error)
        except Exception as exp:
            error = "Unknown error while logging into USGS - {exp}".format(exp = exp)
            logger.error(error)
            log.error(history.SYS, error)

        retry += 1
        if retry > settings.MAX_RECONNECT:
            remaining_time = datetime.timedelta(**settings.HARVEST_WINDOW)
            remaining_seconds = remaining_time.seconds + (remaining_time.days * 24 * 60 * 60)
            logger.info("--Sleeping for {s} seconds...".format(s = remaining_seconds))
            time.sleep(remaining_seconds)
        else:
            time.sleep(10) # 10 seconds
    
    _apiKey = response["data"]
    _aquired = datetime.datetime.utcnow()
    
    return _apiKey


def logout(log = None, force = False):
    global _apiKey
    global _aquired
    
    if not force and _aquired + datetime.timedelta(hours = 1) > datetime.datetime.utcnow():
        return
        
    logger = history.GetLogger()
    if not log:
        log = history.Record()

    response = ""
    url = "https://{usgs_host}/inventory/json/{request_code}".format(usgs_host = settings.USGS_HOST,
                                                           request_code = "logout")
    
    logger.info("Logging out of USGS")

    try:
        response = requests.post(url, data = { 'jsonRequest': json.dumps({ "apiKey": _apiKey }) }, timeout = settings.TIMEOUT)
        response = response.json()
        
        if response["errorCode"]:
            error = "Error from USGS while logging out - {err}".format(err = response["error"])
            logger.error(error)
            log.error(history.SYS, error)
    
    except requests.exceptions.RequestException as exp:
        error = "Failed to contact USGS - {exp}".format(exp = exp)
        logger.error(error)
        log.error(history.SYS, error)
        return False
    except ValueError as exp:
        error = "Error while decoding response - {exp}".format(exp = exp)
        logger.error(error)
        log.error(history.SYS, error)
        return False
    except Exception as exp:
        error = "Unknown error while logging into USGS - {exp}".format(exp = exp)
        logger.error(error)
        log.error(history.SYS, error)
        return False
    
    _apiKey = ""
    
    return True
    

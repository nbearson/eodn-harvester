# =============================================================================
#  EODNHarvester
#
#  Copyright (c) 2015-2016, Trustees of Indiana University,
#  All rights reserved.
#
#  This software may be modified and distributed under the terms of the BSD
#  license.  See the COPYING file for details.
#
#  This software was created at the Indiana University Center for Research in
#  Extreme Scale Technologies (CREST).
# =============================================================================
import smtplib
from email.mime.text import MIMEText
import datetime
import logging
import requests
import subprocess
import random
import filecmp
import json
import os
import csv
import time

import eodnharvester.settings as settings
import eodnharvester.auth as auth
import eodnharvester.history as history

def runner(hour):
    reported = False
    logger = history.GetLogger()
    
    while True:
        now = datetime.datetime.utcnow()
        if (now.hour == settings.REPORT_HOUR and not reported) or settings.FORCE_EMAIL:
            logger.debug("\033[94m{r}{f}\033[0m".format(r = "Creating report...", f = " FORCED" if settings.FORCE_EMAIL else ""))
            try:
                if os.path.isfile("{ws}/harvest.tmp".format(ws = settings.WORKSPACE)):
                    with open("{ws}/harvest.tmp".format(ws = settings.WORKSPACE), 'r+') as f:
                        logger.debug("\033[94m{r}\033[0m".format(r = "  Reading report..."))
                        reader = csv.reader(f)
                        report = list(map(lambda vs: { "ts": vs[0], "scene": vs[1], "code": vs[2], "filesize": vs[3], "speed": vs[4], "usgs_live": vs[5], "eodn_live": vs[6] }, reader))
                        if report:
                            logger.debug("\033[94m{r}\033[0m".format(r = "  Found report..."))
                            logger.debug("\033[1m{r}\033[0m".format(r = report))
                            body = write_report(report)
                            send_mail(body)
                            logger.debug("\033[1m{r}\033[0m".format(r = body))
            
                    os.remove("{ws}/harvest.tmp".format(ws = settings.WORKSPACE))
                logger.debug("\033[94m{r}\033[0m".format(r = "Report complete"))
            except Exception as exp:
                logger.debug("\033[91m{r} - {e}\033[0m".format(r = "Error in reporter", e = exp))
            reported = True
        else:
            reported = False
        time.sleep(5)
        
def write_report(report):
    logger = history.GetLogger()
    now = datetime.datetime.utcnow()
    last = now - datetime.timedelta(hours = 24)
    harvested_size = 0
    
    product_list = list(filter(lambda v: v["code"] == "STANDARD", report))
    if product_list:
        sample_product = random.choice(product_list)
    
    body  = """
    <html>
      <head></head>
      <body>
      <p>The following files were harvested from {0} to {1}:</p>
      <br><br>
      <table>
      <tr>
        <th style='padding: 5px 10px'>Scene</th>
        <th style='padding: 5px 10px'>Product Code</th>
        <th style='padding: 5px 10px'>Size (MB)</th>
        <th style='padding: 5px 10px'>Download Speed</th>
        <th style='padding: 5px 10px'>EODN Live</th>
      </tr>
    """.format(last.strftime("%Y-%m-%d %H:%M:00"), now.strftime("%Y-%m-%d %H:%M:00"))
    
    for o in report:
        harvested_size += int(o["filesize"])
        size = float(o["filesize"]) / float(2**20)
        body += "<tr>"
        body +=   "<td style='padding: 2px 5px'>{scene}</td>".format(scene   = o["scene"])
        body +=   "<td style='padding: 2px 5px'>{code}</td>".format(code     = o["code"])
        body +=   "<td style='padding: 2px 5px'>{size:.2f}</td>".format(size = size)
        body +=   "<td style='padding: 2px 5px'>{speed}</td>".format(speed = o["speed"])
        body +=   "<td style='padding: 2px 5px'>{ts}</td>".format(ts = o["eodn_live"])
        body += "</tr>"
    body += "</table><br>  Total size: {size:.2f} MB<br><br>".format(size = float(harvested_size) / (2**20))
    
    validated = False
    if product_list:
        logger.warn("--Validating random download: {product}".format(product = sample_product["scene"]))
        validated, error = validate_product(sample_product)
        body += "<p>Sample product {product}... [<span style='color:{color}'>{valid}</span>] {cause}</p>".format(product = sample_product["scene"],
                                                                                                                 color   = "green" if validated else "red",
                                                                                                                 valid   = "PASS" if validated else "FAIL",
                                                                                                                 cause   = error)
        
    body += "</html></body>"
    
    return body

def send_mail(report):
    mail_from = "no-reply@data-logistics.org"
    
    msg = MIMEText(report.encode('utf-8'), 'html', 'utf-8')
    msg['Subject'] = "Daily Harvest Report [{name}]".format(name = settings.HARVEST_NAME)
    msg['From']    = mail_from
    msg['To']      = settings.REPORT_EMAIL

    server = smtplib.SMTP('localhost')
    server.sendmail(mail_from, [settings.REPORT_EMAIL], msg.as_string())
    server.quit()


def validate_product(product):
    tmpDiff      = 0
    eodn_file    = "{workspace}/{filename}".format(workspace = settings.WORKSPACE, filename = "validate_eodn.tar.gz")
    source_file  = "{workspace}/{filename}".format(workspace = settings.WORKSPACE, filename = "validate_source.tar.gz")
    
    if not download_eodn(product, eodn_file):
        return False, "Failed to download from EODN"
        
    if not download_source(product, source_file):
        return False, "Failed to download from USGS"
        
    if os.path.getsize(source_file) != os.path.getsize(eodn_file):
        os.remove(source_file)
        os.remove(eodn_file)
        return False, "USGS and EODN files not equal [length]: USGS - {usgs_size} kB,  EODN - {eodn_size} kB".format(usgs_size = os.path.getsize(source_file),
                                                                                                                     eodn_size = os.path.getsize(eodn_file))
        
    with open(source_file, 'rb') as fp_source:
        with open(eodn_file, 'rb') as fp_eodn:
            tmpSourceData = fp_source.read(settings.VALIDATION_GRANULARITY)
            tmpEodnData   = fp_eodn.read(settings.VALIDATION_GRANULARITY)
            
            while tmpSourceData:
                if tmpSourceData != tmpEodnData:
                    tmpDiff += settings.VALIDATION_GRANULARITY
                    
                tmpSourceData = fp_source.read(settings.VALIDATION_GRANULARITY)
                tmpEodnData   = fp_eodn.read(settings.VALIDATION_GRANULARITY)
                
    os.remove(source_file)
    os.remove(eodn_file)
    
    if tmpDiff:
        return False, "USGS and EODN files not equal [bytewise]: {size.2}kB diff".format(size = tmpDiff / 1024)
    
    return True, ""



def download_source(product, dest_file):
    logger = history.GetLogger()
    url = "http://{usgs_host}/inventory/json/{request_code}".format(usgs_host    = settings.USGS_HOST,
                                                                    request_code = "download")
    tmpURL = ""
    apiKey = auth.login()
    downloadRequest = {
        "datasetName": settings.DATASET_NAME,
        "apiKey":      apiKey,
        "node":        settings.NODE,
        "entityIds":   product["scene"],
        "products":    ["STANDARD"]
    }
    try:
        entity_data = requests.get(url, params = { 'jsonRequest': json.dumps(downloadRequest) }, timeout = settings.TIMEOUT)
        entity_data = entity_data.json()
        
        if entity_data["errorCode"]:
            error = "Recieved error from USGS - {err}".format(err = entity_data["error"])
            logger.warn(error)
            auth.logout()
            return False
    except requests.exceptions.RequestException as exp:
        logger.warn("Failed to get entity metadata - {exp}".format(exp = exp))
        auth.logout()
        return False
    except ValueError as exp:
        logger.warn("Error while decoding entity json - {exp}".format(exp = exp))
        auth.logout()
        return False
    except Exception as exp:
        logger.warn("Unknown error while getting entity metadata - {exp}".format(exp = exp))
        auth.logout()
        return False
    
    try:
        if len(entity_data["data"]) == 0:
            raise Exception("No download URL recieved")
        tmpURL = entity_data["data"][0]
    except Exception as exp:
        logger.warn("Recieved bad download data from USGS - {exp}".format(exp = exp))
        auth.logout()
        return False


    try:
        response = requests.get(tmpURL, stream = True, timeout = settings.TIMEOUT)
    except requests.exceptions.RequestException as exp:
        error = "Failed to connect to download service - {exp}".format(exp = exp)
        logger.warn(error)
        return False
    except Exception as exp:
        error = "Unknown error while downloading file - {exp}".format(exp = exp)
        logger.warn(error)
        return False

    try:
        with open(dest_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size = settings.DOWNLOAD_CHUNKSIZE):
                if not chunk:
                    continue
                
                f.write(chunk)
                f.flush()
    except Exception as exp:
        error = "Unknown error while opening and storing file - {exp}".format(exp = exp)
        logger.warn(error)
        return False
        
    auth.logout()
    return True




def download_eodn(product, dest_file):
    logger = history.GetLogger()
    url = "{protocol}://{host}:{port}/exnodes?metadata.productCode={code}&metadata.scene={scene}".format(protocol = "https" if settings.USE_SSL else "http",
                                                                                                         host     = settings.UNIS_HOST,
                                                                                                         port     = settings.UNIS_PORT,
                                                                                                         code     = product["code"],
                                                                                                         scene    = product["scene"])
    
    try:
        response = requests.get(url, cert = (settings.SSL_OPTIONS["cert"], settings.SSL_OPTIONS["key"]))
        response = response.json()[0]
    except requests.exceptions.RequestException as exp:
        error = "Failed to connect to UNIS - {exp}".format(exp = exp)
        logger.warn(error)
        return False
    except ValueError as exp:
        error = "Error while decoding unis json - {exp}".format(exp = exp)
        logger.warn(error)
        return False
    except Exception as exp:
        error = "Unknown error while contacting UNIS - {exp}".format(exp = exp)
        logger.warn(error)
        return False

    if response:
        lors_url = response["selfRef"]
        call = subprocess.Popen(['lors_download', 
                                 '-o', dest_file, lors_url], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        
        out, err = call.communicate()
        result   = call.returncode
        if settings.VERBOSE:
            if out:
                logger.warn(out.decode('utf-8'))
            if err:
                logger.warn(err.decode('utf-8'))

        return True
    else:
        return False    

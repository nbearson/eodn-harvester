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

import eodnharvester.settings as settings
import eodnharvester.auth as auth
import eodnharvester.history as history

last_reported = datetime.datetime.utcnow()

def CreateReport(report):
    global last_reported
    now = datetime.datetime.now()
    
    if now.hour >= settings.REPORT_HOUR and now.hour <= settings.REPORT_HOUR + 2 and now - last_reported > datetime.timedelta(hours = 12):
        body = write_report(report)
        send_mail(body)
        last_reported = datetime.datetime.utcnow()

        return body


def write_report(report):
    global last_reported
    logger = history.GetLogger()
    product_list = list(report._record.keys())
    now = datetime.datetime.utcnow()
    harvested_size = 0
    error_list = {}

    if history.SYS in product_list:
        product_list.remove(history.SYS)

    product_list = list(filter(lambda product: product.endswith(".tar.gz") and "complete" in report._record[product] and datetime.datetime.strptime(report._record[product]["ts"], "%Y-%m-%d %H:%M:%S") > last_reported,
                               product_list))
    if product_list:
        sample_product = random.choice(product_list)
    else:
        sample_product = ""
        
    body = "The following files were harvested from {0} to {1}:<br><br><table>".format(last_reported.strftime("%Y-%m-%d %H:%M:00"), now.strftime("%Y-%m-%d %H:%M:00"))
    body += "<tr><th>Filename</th><th>Size (MB)</th><th>Download Speed</th></tr>"

    for key, product in report._record.items():
        try:
            if key == history.SYS:
                continue
                
            ts = datetime.datetime.strptime(product["ts"], "%Y-%m-%d %H:%M:%S")
            if ts > last_reported:
                harvested_size += int(product["filesize"])
                size = float(product["filesize"]) / float(2**20)
                
                body += "<tr><td>{name}</td><td>{size:.2f}</td><td>{speed}</td><td>{ts}</td></tr>".format(name  = key,
                                                                                                          size  = size,
                                                                                                          speed = product["download_speed"],
                                                                                                          ts    = product["ts"])

            if "errors" in product:
                for error in product["errors"]:
                    for err_ts, value in error.items():
                        ts = datetime.datetime.strptime(err_ts, "%Y-%m-%d %H:%M:%S")
                        if ts > last_reported:
                            if key not in error_list:
                                error_list[key] = []
                            error_list[key].append(value)
        
        except Exception as exp:
            logger.warn("Error in report - {exp}".format(exp = exp))

    body += "</table><br>  Total size: {size:.2f} MB<br><br>".format(size = float(harvested_size) / (2**20))

    validated = False
    if sample_product:
        logger.warn("--Validating random download: {product}".format(product = sample_product))
        validated, error = validate_product(sample_product)
        body += "<p>Sample product {product}... [<span style='color:{color}'>{valid}</span>] {cause}</p>".format(product = sample_product,
                                                                                                                 color   = "green" if validated else "red",
                                                                                                                 valid   = "PASS" if validated else "FAIL",
                                                                                                                 cause   = error)
        
    if error_list:
        body += "<br><br><br>The following errors occured during harvesting:<br><table>"

        for key, errors in error_list.items():
            try:
                body += "<tr><td>{product}</td><td style='padding:10px'>".format(product = key)
                
                for error in errors:
                    body += "{error}<br>".format(error = error)
                
                body += "</td></tr>"
            except Exception as exp:
                logger.warn("Error in report pass 2 - {exp}".format(exp = exp))

    body += "</table>"

    return body


def send_mail(report):
    mail_from = "no-reply@data-logistics.org"
    
    msg = MIMEText(report, 'html')
    msg['Subject'] = "Daily Harvest Report"
    msg['From']    = mail_from
    msg['To']      = settings.REPORT_EMAIL

    server = smtplib.SMTP('localhost')
    server.sendmail(mail_from, [settings.REPORT_EMAIL], msg.as_string())
    server.quit()


def validate_product(product):
    tmpDiff      = 0
    eodn_file    = "{workspace}/{filename}".format(workspace = settings.WORKSPACE, filename = "validate_eodn.tar.gz")
    source_file  = "{workspace}/{filename}".format(workspace = settings.WORKSPACE, filename = "validate_source.tar.gz")

    if not download_source(product, source_file):
        return False, "Failed to download from USGS"

    if not download_eodn(product, eodn_file):
        return False, "Failed to download from EODN"

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
        "entityIds":   product.split(".", 1)[0:1],
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
    url = "http://{host}:{port}/exnodes?name={product}".format(host    = settings.UNIS_HOST,
                                                               port    = settings.UNIS_PORT,
                                                               product = product)
    
    try:
        response = requests.get(url)
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
        error = "Unkown error while contacting UNIS - {exp}".format(exp = exp)
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

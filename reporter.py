import smtplib
from email.mime.text import MIMEText
import json
import argparse
import sys
import urllib2
import datetime

from settings import config


last_reported = datetime.datetime.now()

def CreateReport(history):
    global last_reported
    
    
    if datetime.datetime.utcnow() - last_reported > datetime.timedelta(**config['report_period']):
        report = write_report(history)
        self.send_mail(report)
        last_reported = datetime.datetime.now()


def write_report(history):
    global last_reported
    harvested_size = 0
    report = "The following files were harvested from {0} to {1}:<br><br><table>".format(now.strftime("%m-%d-%Y %H:%M:00"), (now - datetime.timedelta(hours = args.duration)).strftime("%m-%d-%Y %H:%M:00"))
    report += "<tr><th>Filename</th><th>Size (MB)</th></tr>"

    for key, run in history.iteritems():
        record_time = datetime.datetime.strptime(key, "%m-%d-%Y %H:%M:%S")
        
        if record_time > last_reported:
            for exnode in run:
                enode_data = get_exnode(exnode["name"])
                if not exnode_data:
                    continue
                    
                harvested_size += exnode_data["size"]
                report = "<tr><td>{name}</td><td>{size}</td></tr>".format(name =  exnode_data["name"],
                                                                          size =  exnode_data["size"] / (2**20))


    report = report + "\n  Total size: {size} MB".format(size = harvested_size / (2**20))

    return report



def get_exnode(name):
    global harvested_size
    url = "{host}:{port}/{collection}?{options}".format(host = config["unis_host"],
                                                        port = config["unis_port"],
                                                        collection = "exnodes",
                                                        options = "name=" + name)
    try:
        request = urllib2.Request(url)
        request.add_header("Accept", "application/perfsonar+json")
    
        response = json.loads(urllib2.urlopen(request, timeout = 20).read())[0]
    except Exception as e:
        print "Failed to retrieve exnode - {0}".format(e)
        return None
    return response


def send_mail(report):
    mail_from = "no-reply@data-logistics.org"
    
    msg = MIMEText(report, 'html')
    msg['Subject'] = "Daily Harvest Report"
    msg['From']    = mail_from
    msg['To']      = config["report-email"]

    server = smtplib.SMTP('localhost')
    server.sendmail(mail_from, [config["report-email"]], msg.as_string())
    server.quit()

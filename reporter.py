import smtplib
from email.mime.text import MIMEText
import json
import argparse
import sys
import urllib2
import datetime

from settings import config


last_reported = datetime.datetime.utcnow()

def StartupReport():
    start_time = datetime.datetime.utcnow()
    report = "<h1>EODN Harvester Startup</h1><h3>{start_time}</h3><br><br>".format(start_time = start_time)
    report += "<div><p>With</p><div style='padding-left:10'>"
    
    report += "<p>Source: {source}</p>".format(source = config['usgs_url'])
    report += "<p>Lower Left [lat/lon]: {ll}</p>".format(ll = config['ll'])
    report += "<p>Upper Right [lat/lon]: {source}</p>".format(source = config['ur'])
    report += "<p>Harvest Period: {source}</p>".format(source = config['harvest_window'])
    report += "<p>Report Period: {source}</p>".format(source = config['report_period'])
    report += "<p>UNIS URL: {source}:{port}</p>".format(source = config['unis_host'], port = config['unis_port'])
    report += "<p>Initial Resource Duration: {source}</p>".format(source = config['lors_duration'])

    report += "</div></div>"

    send_mail(report)

def CreateReport(history):
    global last_reported
    
    
    if datetime.datetime.utcnow() - last_reported >= datetime.timedelta(**config['report_period']):
        report = write_report(history)
        send_mail(report)
        print report
        last_reported = datetime.datetime.utcnow()


def write_report(history):
    global last_reported
    now = datetime.datetime.utcnow()
    harvested_size = 0
    report = "The following files were harvested from {0} to {1}:<br><br><table>".format(last_reported.strftime("%m-%d-%Y %H:%M:00"), now.strftime("%m-%d-%Y %H:%M:00"))
    report += "<tr><th>Filename</th><th>Size (MB)</th></tr>"

    for key, run in history.iteritems():
        record_time = datetime.datetime.strptime(key, "%m-%d-%Y %H:%M:%S")
        
        if record_time >= last_reported:
            for exnode in run:
                harvested_size += exnode["size"]
                report += "<tr><td>{name}</td><td>{size:.2f}</td></tr>".format(name =  exnode["name"],
                                                                          size =  float(exnode["size"]) / (2**20))


    report = report + "<br>  Total size: {size:.2f} MB".format(size = float(harvested_size) / (2**20))

    return report


def send_mail(report):
    mail_from = "no-reply@data-logistics.org"
    
    msg = MIMEText(report, 'html')
    msg['Subject'] = "Daily Harvest Report"
    msg['From']    = mail_from
    msg['To']      = config["report_email"]

    server = smtplib.SMTP('localhost')
    server.sendmail(mail_from, [config["report_email"]], msg.as_string())
    server.quit()





def UnitTests():
    global last_reported

    last_reported = last_reported - datetime.timedelta(**config['report_period'])
    now = datetime.datetime.utcnow()
    history = { 
        now.strftime("%m-%d-%Y %H:%M:%S"): 
        [
            { "name": "Test1", "size": 1000000 },
            { "name": "Test2", "size": 5000 }
        ]
    }

    CreateReport(history)



if __name__ == "__main__":
    UnitTests()

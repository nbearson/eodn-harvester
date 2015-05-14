import smtplib
from email.mime.text import MIMEText
import json
import argparse
import sys
import urllib2
import datetime

from settings import config


def get_exnode(name):
    global harvested_size
    url = "{host}:{port}/{collection}?{options}".format(host = config["unis_host"],
                                                        port = config["unis_port"],
                                                        collection = "exnodes",
                                                        options = "name=" + name)
    request = urllib2.Request(url)
    request.add_header("Accept", "application/perfsonar+json")
    
    response = json.loads(urllib2.urlopen(request, timeout = 20).read())[0]
    return response


def main():
    harvested_size = 0
    mail_from = "no-reply@data-logistics.org"

    description = """{prog} examines the log of recently harvested scenes and
emails an admin a summary""".format(prog = sys.argv[0])
    parser = argparse.ArgumentParser(description = description)
    parser.add_argument('--duration', type=int)
    args = parser.parse_args()

    now = datetime.datetime.utcnow()
    report = "The following files were harvested from {0} to {1}:<br><br><table>".format(now.strftime("%m-%d-%Y %H:%M:00"), (now - datetime.timedelta(hours = args.duration)).strftime("%m-%d-%Y %H:%M:00"))
    report += "<tr><th>Filename</th><th>Size (MB)</th></tr>"
    records = []

    with open(config['history']) as f:
        history = json.loads(f.read())


    for key, run in history.iteritems():
        record_time = datetime.datetime.strptime(key, "%m-%d-%Y %H:%M:%S")
        
        if now - record_time < datetime.timedelta(hours = args.duration):
            for exnode in run:
                records.append(exnode["name"])


    
    print report
    for exnode in records:
        exnode_data = get_exnode(exnode)
        harvested_size += exnode_data["size"]
        report = "<tr><td>{name}</td><td>{size}</td></tr>".format(name =  exnode_data["name"],
                                                                  size =  exnode_data["size"] / (2**20))

    report = report + "\n  Total size: {size} MB".format(size = harvested_size / (2**20))

    msg = MIMEText(report, 'html')
    msg['Subject'] = "Daily Harvest Report"
    msg['From']    = mail_from
    msg['To']      = config["report-email"]

    server = smtplib.SMTP('localhost')
    server.sendmail(mail_from, [config["report-email"]], msg.as_string())
    server.quit()

if __name__ == "__main__":
    main()

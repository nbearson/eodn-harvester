import smtplib
from email.mime.text import MIMEText
import datetime
import logging

import settings
import history

last_reported = datetime.datetime.utcnow()

def StartupReport():
    start_time = datetime.datetime.utcnow()
    body = "<h1>EODN Harvester Startup</h1><h3>{start_time}</h3><br><br>".format(start_time = start_time)
    body += "<div><p>With</p><div style='padding-left:10'>"
    
    body += "<p>Source: {source}</p>".format(source = settings.DATASET_NAME)
    body += "<p>Lower Left [lat/lon]: {ll}</p>".format(ll = settings.LOWER_LEFT)
    body += "<p>Upper Right [lat/lon]: {source}</p>".format(source = settings.UPPER_RIGHT)
    body += "<p>Harvest Period: {source}</p>".format(source = settings.HARVEST_WINDOW)
    body += "<p>Report Period: {source}</p>".format(source = settings.REPORT_PERIOD)
    body += "<p>UNIS URL: {source}:{port}</p>".format(source = settings.UNIS_HOST, port = settings.UNIS_PORT)
    body += "<p>Initial Resource Duration: {source}</p>".format(source = settings.LoRS["duration"])

    body += "</div></div>"

    send_mail(body)

def CreateReport(report):
    global last_reported
    
    
    if datetime.datetime.utcnow() - last_reported >= datetime.timedelta(**settings.REPORT_PERIOD):
        body = write_report(report)
        send_mail(body)
        last_reported = datetime.datetime.utcnow()
        return body


def write_report(report):
    global last_reported
    now = datetime.datetime.utcnow()
    harvested_size = 0
    error_list = {}
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

            for error in product["errors"]:
                for err_ts, value in error.items():
                    ts = datetime.datetime.strptime(err_ts, "%Y-%m-%d %H:%M:%S")
                    if ts > last_reported:
                        if key not in error_list:
                            error_list[key] = []
                        error_list[key].append(value)
                
        
        except Exception as exp:
            logging.warn("Error in report - {exp}".format(exp = exp))

    body += "</table><br>  Total size: {size:.2f} MB<br><br>".format(size = float(harvested_size) / (2**20))

    if error_list:
        body += "The following errors occured during harvesting:<br><table>"

        for key, errors in error_list.items():
            try:
                body += "<tr><td>{product}</td><td style='padding:10px'>".format(product = key)
                
                for error in errors:
                    body += "{error}<br>".format(error = error)
                
                body += "</td></tr>"
            except Exception as exp:
                logging.warn("Error in report pass 2 - {exp}".format(exp = exp))

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





def UnitTests():
    import history
    global last_reported

    last_reported = datetime.datetime.utcnow() - datetime.timedelta(**settings.REPORT_PERIOD) - datetime.timedelta(minutes = 1)
    log = history.GetHistory()
    print(CreateReport(log))



if __name__ == "__main__":
    UnitTests()

#!/usr/bin/python
# EODN_Harvest_0_1_18.py
#
# Harvest data from USGS EROS
#   and populate EODN
#
#   Requires: libxslt-devel, lxml, suds-jurko
#   Requires a login account with EROS
#
#   Change Log:
#       2/14/2015   Add Alternate path for UNIS uploads - Jeremy Musser
#       9/2014      Initial Coding - PR Blackwell
#       9/9/2014    fix_wsdl and other wizardry - Chris Brumgard
#       9/11/2014   Added factory constructs - CB
#       9/12/2014   Refactored Classes - PRB
#                   Added "TEST" mode - PRB
#                   Documentation - PRB
#       9/13/2014   Removed scene class - PRB
#       9/15/2014   Added argparse - PRB
#                   Implented batches - PRB
#                   Added Process helper class - PRB
#       9/16/2014   Add Config file - PRB
#       9/17/2014   Mics bug fixes - PRB
#       9/18/2014   Refactored search - PRB
#       9/21/2014   Refactored main - PRB
#       9/22/2014   Added error handling - PRB
#       9/25/2014   Enhanced error handling - PRB
#       9/26/2014   Added error handling - PRB
#       9/28/2014   Added moving window - PRB
#       9/29/2014   Added download metrics - PRB
#       10/1/2014   Added start date - PRB
#       10/3/2014   I/O error handling - PRB
#       10/6/2014   Fixed batch bug -- PRB
#       10/8/2014   Added 'bad-file list -- PRB
#       10/9/2014   Added AVMSS ingest initiate -- PRB
#       10/15/2014  Added Random upload depot -- Chris Brumgard
#       12/15/2014  Refactored main -- PRB
#       12/16/2014  Fix bug in AVMSS upload -- PRB
#__author__ = 'prb'

# imports
import sys
import os.path
import datetime
from datetime import timedelta
import argparse
import getpass
import io
import logging
import time
from subprocess import call
import urllib2
import shutil
import ntpath
from urllib2 import urlopen
import random
import re
import unisencoder.dispatcher as unisDispatch
from suds.client import Client
from lxml import etree

import settings

# logging.basicConfig(level=logging.INFO)
if __debug__:
    logging.getLogger('suds.client').setLevel(logging.DEBUG)
else:
    logging.getLogger('suds.client').setLevel(logging.CRITICAL)

# read config file
config = settings.config
#config = {}
#with open('./harvest.cfg') as f:
#    for line in f:
#        li = line.strip()
#        if not ((li.startswith('#')) or (li == '')):
#            key = li.split(' ')[0]
#            value = li.split(' ')[1]
#            if key == 'start' and value != 'None':
#                value = value + ' ' + li.split(' ')[2]
#            if value == 'True':
#                config[key] = True
#            elif value == 'False':
#                config[key] = False
#            elif value == 'None':
#                config[key] = None
#            else:
#                config[key] = value

retry = int(config['retry'])
downloads = 0
download_speeds = 0
DEPOTS = (
   #('depot1.loc1.sfasu.reddnet.org',6714),
   ('depot1.loc1.ufl.reddnet.org',6714),
   ('depot1.loc1.utk.reddnet.org',6714),
   ('depot10.loc1.utk.reddnet.org',6714),
   ('depot2.loc1.utk.reddnet.org',6714),
   #('depot3.loc1.sfasu.reddnet.org',6714),
   #('depot3.loc1.ufl.reddnet.org',6714),
   #('depot3.loc1.utk.reddnet.org',6714),
   #('depot4.loc1.sfasu.reddnet.org',6714),
   #('depot4.loc1.utk.reddnet.org',6714),
   #('depot5.loc1.sfasu.reddnet.org',6714),
   ('depot5.loc1.ufl.reddnet.org',6714),
   #('depot5.loc1.utk.reddnet.org',6714),
   #('depot6.loc1.sfasu.reddnet.org',6714),
   ('depot6.loc1.ufl.reddnet.org',6714),
   #('depot7.loc1.sfasu.reddnet.org',6714),
   ('depot7.loc1.utk.reddnet.org',6714),
   ('depot8.loc1.utk.reddnet.org',6714),
   #('depot9.loc1.utk.reddnet.org',6714),
   ('dresci.incntre.iu.edu',6714))  #,
   #('reddnet-depot1.reddnet.org',6714),
   #('reddnet-depot10.reddnet.org',6714),
   #('reddnet-depot2.reddnet.org',6714),
   #('reddnet-depot3.reddnet.org',6714),
   #('reddnet-depot4.reddnet.org',6714),
   #('reddnet-depot5.reddnet.org',6714),
   #('reddnet-depot6.reddnet.org',6714),
   #('reddnet-depot7.reddnet.org',6714),
   #('reddnet-depot8.reddnet.org',6714),
   #('reddnet-depot9.reddnet.org',6714),
   #('tvdlnet1.sfasu.edu',6714)),
   #('tvdlnet2.sfasu.edu',6714),
   #('tvdlnet3.sfasu.edu',6714))

parser = argparse.ArgumentParser(description='Retrieve data from EROS and upload to EODN')
parser.add_argument('-a', action='store_true', dest='avmss', default=False, help='AVMSS only')
parser.add_argument('-q', action='store_true', dest='quiet', default=False, help='surpress progress display')
parser.add_argument('-t', action='store_true', dest='test', default=False, help='Disable file I/O')
parser.add_argument('-d', action='store_true', dest='download', default=False, help='Download only mode')
parser.add_argument('-e', action='store_true', dest='eodn', default=False, help='Supress download from USGS')
parser.add_argument('-f', action='store_true', dest='force', default=False, help='Suppress file exists tests')
parser.add_argument('-g', action='store_true', dest='glovis', default=False, help='Glovis only mode')
parser.add_argument('-b', action='store', dest='bad_file_list', default=None, help="Supply list of files to reload")
args = parser.parse_args()

quiet_mode = args.quiet

if args.test:
    print('Running in TEST mode - download, lors_upload and lodn_importExnode disabled...')
    test_mode = True
    #config['file_stat'] = True
    config['file_donwload'] = True
    config['lors_upload'] = False
    config['lodn_stat'] = False
    config['lodn_import'] = False
# download_mode = args.download
if args.download:
    print('Running in DOWNLOAD mode - files will be downloaded and saved but not sent to EODN...')
    config['file_stat'] = True
    config['file_donwload'] = True
    config['lors_upload'] = False
    config['lodn_stat'] = False
    config['lodn_import'] = False
    config['file_delete'] = False
# eodn_mode = args.eodn
if args.eodn:
    print('Running in EODN ONLY mode - files will not be downloaded.  Local files necessary...')
    config['file_stat'] = False
    config['file_donwload'] = False
    config['lors_upload'] = True
    config['lodn_stat'] = True
    config['lodn_import'] = True
#force_mode = args.force
if args.force:
    print('Running in FORCE mode - all files will be downloaded and sent to EODN...')
    config['file_stat'] = False
    config['lodn_stat'] = False

if args.glovis:
    # Glovis only mode all other i/o disabled
    print('Running in GloVis only mode - all other I/O is disabled')
    config['file_stat'] = False
    config['file_download'] = False
    config['lors_upload'] = False
    config['Lodn_stat'] = False
    config['lodn_import'] = False
    config['file_delete'] = False
    config['glovis'] = True
    config['avmss'] = False

if args.avmss:
    # AVMSS only mode all other i/o disabled
    print('Running in AMVSS only mode - other upload is disabled')
    config['file_stat'] = True
    config['file_download'] = True
    config['lors_upload'] = False
    config['lodn_stat'] = False
    config['lodn_import'] = False
    config['glovis'] = False
    config['avmss'] = True

if args.bad_file_list is not None:
    config['bad_files'] = True
    config['bad_file_list'] = args.bad_file_list
    config['file_stat'] = True
    config['file_download'] = True
    config['lodn_stat'] = False
    config['lors_upload'] = True
    config['file_delete'] = False
    config['glovis'] = True
    config['avmss'] = True
else:
    config['bad_files'] = False


class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        if config['logging'] is True:
            self.log = open(config['log_file'], 'w')
            self.log.close()
        if config['error_logging'] is True:
            self.error_log = open(config['error_file'], 'w')
            self.error_log.close()

    def write(self, message):
        time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not quiet_mode:
            self.terminal.write(str(message) + '\n')
        if config['logging']:
            self.log = open(config['log_file'], 'a')
            self.log.write(time_stamp + ' ' + str(message) + '\n')
            self.log.close()

    def write_error(self, message):
        time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #print('error_logging: ' + str(config['error_logging']))
        if config['error_logging']:
            self.error_log = open(config['error_file'], 'a')
            self.error_log.write(time_stamp + ' ' + str(message) + '\n')
            self.error_log.close()

    def close(self):
        if config['logging']:
            self.log.close()
        if config['error_logging']:
            self.error_log.close()


class Product(object):
    # Base class for scene products
    # Create subclasses for different datasets
    def __init__(self, **kwargs):
        self.attribs = kwargs
        self.attribs['available'] = False
        #self.attribs['download_code'] = None
        #self.attribs['productName'] = None
        #self.attribs['file_size'] = None

    def set_attribs(self, k, v):
        self.attribs[k] = v

    def get_attribs(self, k):
        return self.attribs.get(k, None)


class Criteria(object):  # base class for additionalCriteeria
    # Define sub classes for other datasets
    def __init__(self, **kwargs):
        self.attribs = kwargs
        self.attribs['fieldID'] = None
        self.attribs['name'] = None
        self.attribs['fieldLink'] = None
        self.attribs['valueList'] = None

    def set_attribs(self, k, v):
        self.attribs[k] = v

    def get_attribs(self, k):
        return self.attribs.get(k, None)


class CriteriaL8(Criteria):  # Currently not used...
    # Scene sub class Landsat 8 data - inherits from Search
    def __init__(self, *args, **kwargs):
        try:
            self._w = kwargs.pop('w')
        except KeyError:
            pass
        super(CriteriaL8, self).__init__()

        self.attribs['data_set'] = 'Landsat_8'


class Search(object):
    # Base class for inventory searches
    # Create sub classes for other datasets
    def __init__(self, **kwargs):
        self.attribs = kwargs
        self.attribs['client_obj'] = None  # the SOAP client for the search
        self.attribs['data_set'] = None  # this gets set in sub classes for each data_set type
        self.attribs['end_date'] = datetime.datetime.now()  # ending date - default now
        # convert start date to datetime format or caculate as offset from Now.
        if config['start'] is None:
            self.attribs['start_date'] = datetime.datetime.now() - timedelta(days=int(config['days']))
        else:
            self.attribs['start_date'] = datetime.datetime.strptime(config['start'], '%Y-%m-%d %H:%M:%S.%f')
        # Have to have at least one day's data to process
        if (self.attribs['end_date'] - self.attribs['start_date']).days == 0:
            self.attribs['start_date'] = self.attribs['start_date'] - timedelta(1, 0, 0)
        self.attribs['sort'] = config['sort']  # Sort order ['ASC', 'DESC'] - default 'ASC'
        self.attribs['criteria'] = None  # config['criteria']  # Additional criteria - default None
        self.attribs['max_recs'] = int(config['max_recs'])  # maximum number of records to return
        self.attribs['startingNumber'] = None # first record to return - default None
        self.attribs['last_rec'] = 0 # last recrod returned - default 0
        self.attribs['node'] = config['node']  # data node to access - [ 'CWIC', 'EE', 'HDDS', 'LPCS'] -  default "EE",
        self.attribs['apikey'] = None  # api authentication key
        self.attribs['data_set'] = config['data_set']  # default to Earth Explorer dataset
        self.attribs['attributes'] = 'entityId'  # the attribute to return - default 'all'

    def set_attribs(self, k, v):
        self.attribs[k] = v

    def get_attribs(self, k):
        return self.attribs.get(k, None)

    def do_search(self):  # searchs for scenes and returns selected attributes

        logger.write('Searching with these parameters:')
        logger.write('  Data set: ' + self.attribs['data_set'])
        logger.write('  Lower left: ' + str(self.attribs['ll']))
        logger.write('  Upper right: ' + str(self.attribs['ur']))
        logger.write('  Start date: ' + str(self.attribs['start_date']))
        logger.write('  End date: ' + str(self.attribs['end_date']))
        logger.write('  Additional criteria: ' + str(self.attribs['criteria']))
        logger.write('  Max records: ' + str(self.attribs['max_recs']))
        logger.write('  Starting number: ' + str(self.attribs['startingNumber']))
        logger.write('  Sort: ' + self.attribs['sort'])
        logger.write('  Node: ' + self.attribs['node'])

        if (self.attribs['client_obj'] is None) or (self.attribs['apikey'] is None):  # check for missing parameters
            logger.write('Missing required parameters - must include client object and apiKey')
            return 'Error'
        else:
            entities = None
            self.attribs['results'] = None
            self.attribs['entities'] = None
            response = self.attribs['client_obj'].service.search(datasetName=self.attribs['data_set'],
                                                                 lowerLeft=self.attribs['ll'],
                                                                 upperRight=self.attribs['ur'],
                                                                 startDate=self.attribs['start_date'],
                                                                 endDate=self.attribs['end_date'],
                                                                 additionalCriteria=self.attribs['criteria'],
                                                                 maxResults=self.attribs['max_recs'],
                                                                 startingNumber=self.attribs['startingNumber'],
                                                                 sortOrder=self.attribs['sort'],
                                                                 node=self.attribs['node'],
                                                                 apiKey=self.attribs['apikey'])
            # populate the search object
            for entry in response:
                self.set_attribs(entry[0], entry[1])
            # populate search.entities list
            entities = self.attribs['client_obj'].factory.create('ArrayOfString')
            results = self.get_attribs('results')
            for result in results:
                #entities.item = [item[self.attribs['attributes']] for item in response.results]
                entities.item.append(result['entityId'])
            self.set_attribs('entities', entities)
            # print(str(entities))


            #entities = self.attribs['client_obj'].factory.create('ArrayOfString')
            #items.item = [item[self.attribs['attributes']] for item in response.results]

            #return items

            return response


class SearchL8(Search):
    # search Landsat 8 data - inherits from Search
    def __init__(self, *args, **kwargs):
        try:
            self._w = kwargs.pop('w')
        except KeyError:
            pass
        super(SearchL8, self).__init__()

        self.attribs['data_set'] = 'Landsat_8'


class Process(object):
    # helper class for processing EROS Data
    # use sub classes to encapsulate specific properties of various data sets
    def __init__(self, **kwargs):
        self.attribs = kwargs

    def set_attribs(self, k, v):
        self.attribs[k] = v

    def get_attribs(self, k):
        return self.attribs.get(k, None)

    # print available services
    def print_services(self, client):
        # list all available operation
        print('Target Namespace: ' + client.namespace)
        print('')
        for service in client.services.values():
            for port in service['ports'].values():
                print('Port Location: ', port['location'])
                for op in port['operations'].values():
                    print('Name: ', op['name'])
                    print('Docs: ', op['documentation'])
                    print('SOAPActions: ', op['action'])
                    print('Imput: ', op['input'])  # args type declaration
                    print('Output: ', op['output'])  # returns type declarations
                    print('')


    def get_service_inventory_criteriafield(self, client, field, value):
        # accept a string field, value pair and return a Service_Class_CriteriaField
        item = client.factory.create('Service_Inventory_CriteriaField')
        #item.fieldId = '10037'
        item.name = field
        item.valueList = value
        #item.fieldLink = "https://lta.cr.usgs.gov/landsat_dictionary.html#cloud_cover"
        queries = client.factory.create('ArrayOfService_Inventory_CriteriaField')

        queries.item = [item]

        return queries

    def get_arrayofservice_inventory_criteriafield(self, client, criteraFields):
        # accept a list of Service_Inventory_CriteriaFields and return an ArrayOfService_InventoryCriteriaField
        results = client.factory.create('ArrayOfServiec_Inventory_CriteriaField')
        for item in criteraFields:
            results.add(item)
        return results

    def get_service_class_coordinate(self, client, lat, long):
        # accept a text coordinate pair and return a Service_Class_Coordinate
        result = client.factory.create('Service_Class_Coordinate')
        result.latitude = lat
        result.longitude = long
        return result

    def deconstruct(self, base):  # Takes a scene name and returns the directory parts
        sensor = base[0:3]  # This probably won't work for other data sets
        path = base[3:6]  # Should be implemented as a Class with specific subclasses for different data sets
        row = base[6:9]
        year = base[9:13]
        doy = base[13:16]
        # convert doy to yyyy-mm-dd
        date = (datetime.datetime(int(year), 1, 1) + datetime.timedelta(int(doy) - 1)).strftime('%Y-%m-%d')
        prod = base[16:]
        if sensor == "LC8":
            sensor = "l8oli"
        path_parts = {'sensor': sensor, 'path': path, 'row': row, 'year': year, 'doy': doy, 'ymd': date, 'prod': prod}
        return path_parts

    def build_lodn_dir(self, product_id):
        parts = self.deconstruct(product_id)

        xnd_path = 'lodn://' + config['lodn_url'] + config['lodn_root']
        call(['lodn_mkdir', xnd_path])
        xnd_path = xnd_path + '/' + config['data_type']
        call(['lodn_mkdir', xnd_path])
        xnd_path = xnd_path + '/p' + parts['path']
        call(['lodn_mkdir', xnd_path])
        xnd_path = xnd_path + '/r' + parts['row']
        call(['lodn_mkdir', xnd_path])
        xnd_path = xnd_path + '/y' + parts['year']
        call(['lodn_mkdir', xnd_path])

    def get_lodn_path(self, product_id):
        parts = self.deconstruct(product_id)
        xnd_path = 'lodn://' + config['lodn_url'] + config['lodn_root'] + '/' + config['data_type'] + '/p' + parts[
            'path'] + '/r' + parts['row'] + '/y' + parts['year'] + '/' + product_id
        return xnd_path

    def get_dataset_fields(self, client, apikey):
        response = client.service.datasetFields(datasetName=config['data_set'], apiKey=apikey, node=config['node'])
        return response

    def get_download_options(self, client, apikey, uname, pword, entity_ids, product_code):  # returns a list product objects
        products = []
        response = '0'
        for i in range(retry):
            if response != '0':
                break

            try:
                response = client.service.downloadOptions(datasetName=config['data_set'], apiKey=apikey,
                                                          node=config['node'], entityIds=entity_ids)
            except Exception as e:
                apikey = None
                logger.write_error('get_url failed: ' + str(e))
                apikey = client.service.login(uname, pword)
                if apikey == None:
                    continue
                logger.write_error('Success: apiKey = ' + str(apikey))

        for record in response:
            for obj in record.downloadOptions:
                product = Product()
                if obj.available == 'False':
                    product.set_attribs('available', False)
                elif obj.available == 'True':  # override available if file is bigger than max_file_size
                    product.set_attribs('available', obj.filesize <= int(config['max_file_size']))
                elif product_code == obj.downloadCode or product_code == 'all':
                    product.set_attribs('available', obj.available)
                    product.set_attribs('download_code', obj.downloadCode)
                    product.set_attribs('productName', obj.productName)
                    product.set_attribs('file_size', obj.filesize)
                    product.set_attribs('url', obj.url)
                    products.append(product)

        return products

    def get_product_codes(self, client, apikey, entity_ids):  # returns a list of downloadable products.
        response = client.service.downloadOptions(datasetName=config['data_set'], apiKey=apikey, node=config['node'],
                                                  entityIds=entity_ids)

        products = client.factory.create('ArrayOfString')
        for r in response:
            if config['small_files']:
                products.item = [o.downloadCode for o in r.downloadOptions if o.downloadCode != 'STANDARD']
            else:
                products.item = [o.downloadCode for o in r.downloadOptions]
        if products is not None:
            return products
        else:
            exit(1)

    def print_download_options(self, product):
        print('available: ', product.get_attribs('available'))
        print('download code: ', product.get_attribs('download_code'))
        print('product name: ', product.get_attribs('productName'))
        print('download url: ', product.get_attribs('url'))

    def get_url(self, client, apikey, entity_ids, product_ids, uname, pword):
        # print('data_set: ', config['data_set'])
        response = '0'
        for i in range(retry):
            if response != '0':
                break
            try:
                response = client.service.download(datasetName=config['data_set'], apiKey=apikey, node=config['node'],
                                                   entityIds=entity_ids, products=product_ids)
            except Exception as e:
                apikey = None
                logger.write_error('get_url failed: ' + str(e))
                if str(e).startswith('Server raised fault:'):
                    continue
                apikey = client.service.login(uname, pword)
                if apikey == None:
                    continue
                logger.write_error('Success: apiKey = ' + str(apikey))

        if len(response) > 0:
            return response.item[0]

    def get_urls(self, client, apikey, product_ids,
                 products):  # returns a array of download URLs for available products
        logger.write('Retrieving URLs')
        response = client.service.download(datasetName=config['data_set'], apiKey=apikey, node=config['node'],
                                           entityIds=product_ids, products=product_ids)
        urls = []
        for r in response.item:
            urls.append(r)

        if len(urls) > 0:
            logger.write('Found ' + str(len(urls)) + ' records.')
            return urls
        else:
            logger.write('Failed to retrieve URLs.')
            exit(1)

    def get_entityid_from_url(self, url):
        parts = url.split('/')
        return parts[5]

    def split_filename(self, entity, part):
        #print('entity: ' + entity)
        parts = entity.split('.')
        #print('parts[0]: ' + parts[0])
        #print('parts[1]: ' + parts[1])
        if len(parts) > 2:
            print('parts[2]: ' + parts[2])
        if parts[0].endswith('_TIR'):
            product_code = 'FR_THERM'
            entity_id = entity[:len(parts[0])-4]
        elif parts[0].endswith('_QB'):
            product_code = 'FR_QB'
            entity_id = entity[:len(parts[0])-3]
        elif len(parts) == 3:
            product_code='STANDARD'
            entity_id = parts[0]
        elif parts[1] == 'zip':
            product_code='FR_BUND'
            entity_id = parts[0]
        else:
            product_code = 'FR_REFL'
            entity_id = parts[0]
        if part == 'entity_id':
            return entity_id
        elif part == 'product_code':
            return product_code


    def get_file_name(self, base_name, download_code):
        if download_code == 'FR_REFL':
            file_name = base_name + '.jpg'
        elif download_code == 'FR_THERM':
            file_name = base_name + '.jpg'
        elif download_code == 'FR_QB':
            file_name = base_name + '.png'
        elif download_code == 'FR_BUND':
            file_name = base_name + '.zip'
        else:
            file_name = base_name
        return file_name

    def file_stat(self, file_path, file_size):
        if os.path.isfile(file_path) is True:
            # if (os.path.getsize(file_path) == file_size) is True:
            return True
        else:
            return False
            #else:
            #        return False

    def get_base_name(self, url):
        #print('get_filename - url: ', url)
        filename = ntpath.basename(url)
        #print('get_filename - filename: ', filename)
        basename = filename.split('?')[0]
        #print('get_filename - basename: ', basename)

        return str(basename)

    def chunk_report(self, bytes_so_far, chunk_size, total_size):
        percent = float(bytes_so_far) / total_size
        percent = round(percent * 100, 2)
        sys.stdout.write("       Downloaded %d of %d bytes (%0.2f%%)\r" %
                         (bytes_so_far, total_size, percent))

        if bytes_so_far >= total_size:
            sys.stdout.write('\n')

    def chunk_read(self, response, file_path, usgs_filesize, chunk_size=8192, report_hook=None):
        #total_size = response.info().getheader('Content-Length').strip()
        if response.info().get('content-length') is None:
            total_size = usgs_filesize
        else:
            total_size = int(response.info().get('content-length'))
        bytes_so_far = 0
        outfile = open(file_path, 'ab')

        while 1:
            chunk = response.read(chunk_size)
            outfile.write(chunk)
            bytes_so_far += len(chunk)

            if not chunk:
                break
            if report_hook:
                report_hook(bytes_so_far, chunk_size, total_size)

        outfile.close()
        #return bytes_so_far
        return total_size

    def download(self, url, file_path, usgs_filesize):
        # downloads the file at the supplied URL
        print('enter Download - line 665')
        global downloads, download_speeds
        response = '0'
        try:
            #print('start download try line 669')
            start = time.clock()
            response = urlopen(url);
            #print('calculate file size line 671')
            file_size = self.chunk_read(response=response, file_path=file_path, usgs_filesize=usgs_filesize,
                                        report_hook=self.chunk_report)
            #print('print file size - line 675')
            logger.write('file size: ' + str(file_size))
            #with open(file_path, 'wb') as f:
            #     f.write(response.read())
            #file_size = int(response.info().get('content-length'))
            # print('file_size: ', str(file_size))
        except Exception as e:
            logger.write(e)
            logger.write_error(e)
            return '-1'

        #print('increment downloads line 684')
        downloads = downloads + 1
        #print('time.clock() - start: ' + str(time.clock() - start))
        if time.clock() - start != 0:
            #print('time.clock start line 687')
            #print('start: ', str(start))
            download_speed = round((file_size / (time.clock() - start)) / 1024, 2)
            download_string = str(download_speed)
            #print('download_string: ', download_string)
            logger.write('       Downloaded at ' + download_string + 'Kb/sec.')
            logger.write('       USGS filesize: ' + str(usgs_filesize) + ',  Actual filesize: ' + str(file_size))
            #logger.write('Downloaded - ' + str((filesize / (time.clock() - start))/1024) + ' Kb/sec')
            download_speeds = download_speeds + download_speed
            #print('download_speeds: ', str(download_speeds))
            downloads = downloads + 1
            #print('downloads: ', str(downloads))
            harvest_metrics = config['workspace'] + 'harvest_metrics.log'
            if config['download_logging']:
                with open(config['download_file'], 'at') as f3:
                    time_stamp = time.strftime('%b %d %Y %H:%M:%S')
                    f3.write(time_stamp + ' ' + str(file_size) + ' ' + download_string + '\n')
        else:
            logger.write_error('*** Download speed calculation failed.')
            #print('leaving download')
        return 0  # don't fail on speed calculation failure!

    def upload_to_eodn(self, xnd_file, file):
        #lors_upload --duration=10h --copies=1 -X /home/prb/.xndrc.$((${RANDOM} % 7)) --depot-list -f $f response = '0'
        results = '0'
        for i in range(retry):
            try:
                results = call(['lors_upload', '--duration=720h', '--none', '-c', '1', '-H', 'dlt.incntre.iu.edu', '-V', '1', '-o', xnd_file, file])
                if results == 0:
                    break
            except Exception as e:
                logger.write_error(e)

        return results

    #def upload_to_eodn(self, xnd_file, file):
    #    # make a new xndrc file with a random depot
    #    generate_random_xndrc()
    #    #lors_upload --duration=10h --copies=1 -X /home/prb/.xndrc.$((${RANDOM} % 7)) --depot-list -f $f
    #    results = call(['lors_upload', '--duration=10h', '--copies=1', '--xndrc=test-xndrc', '--depot-list', '-o', xnd_file, file])
    #    if results != 0:
    #        exit(1)
    #    else:
    #        return 0

    def lodn_stat(self, xnd_path):
        if config['lodn_import']:
            lodn_stat_result = call(['lodn_stat', xnd_path])
            return lodn_stat_result == 0
        else:
            return False

    def import_exnode(self, xnd_path, lodn_path):
        results = 0
        logger.write('       lodn_path: ' + lodn_path)
        logger.write('       xnd_path: ' + xnd_path)
        logger.write('       Unlinking...')
        call(['lodn_unlink', lodn_path])
        logger.write('       Importing exNode.')
        results = call(['lodn_importExnode', xnd_path, lodn_path])  # import the exNode to loDN
        return results

    def unis_import(self, xnd_filename, xnd_path, product_id):
        logger.write('Importing exnode to UNIS')
        scene_id = product_id[0:16]
        dispatch = unisDispatch.Dispatcher()
        unis_root = unisDispatch.create_remote_directory("root", None)
        extended_dir = unisDispatch.parse_filename(xnd_filename)
        
        parent = unisDispatch.create_directories(extended_dir, unis_root)
        dispatch.DispatchFile(xnd_path, parent, metadata = { "scene_id": scene_id })

    def add_to_glovis(self, product_id):
        parts = self.deconstruct(product_id)
        scene_id = self.get_attribs('data_type') + ',' + parts['path'] + ',' + parts['row'] + ',' + parts['ymd']
        glovis_call = scene_id + ' | ' + config['glovis_util']
        if quiet_mode:
            glovis_call = glovis_call + ' < /dev/null'

        result = call(['ssh', '-t', config['glovis_url'], 'source ' + config['glovis_env'] + ' > /dev/null; ' + 'echo',
                       glovis_call])
        #call(['ssh', '-t', config['glovis_url'], 'source ' + config['glovis_env'] +
        #      ' > /dev/null; ' + 'echo', scene_id + ' | ' + config['glovis_util']])
        return result

    def add_to_avmss(self, file_path, file_name):
        # send exnode to the AmericaView Multi-state Server
        #print(str(call(['ssh', '-f', config['avmss_url']])))
        #logger.write('       sending LandsatLook Natural Color product to AVMSS..')
        result = call(['scp', file_path, config['avmss_url'] + ':' + config['avmss_path']])
        file_name = config['avmss_path'] + file_name
        #print('AVMSS_file_name: ' + file_name)
        logger.write('       ssh '+config['avmss_url']+ ':' + config['avmss_cmd']+' '+file_name)
        call(['ssh', config['avmss_url'], config['avmss_cmd'], file_name])
        return result


class ProcessL8(Process):
    def __init__(self, *args, **kwargs):
        try:
            self._w = kwargs.pop('w')
        except KeyError:
            pass
        super(ProcessL8, self).__init__()

        self.attribs['data_set'] = 'Landsat_8'
        self.attribs['data_type'] = config['data_type']


def list_config():
    for setting in config:
        logger.write('  ' + setting + ': ' + str(config[setting]))


def read_bad_file_list(client, bad_file_list):
    entities = client.factory.create('ArrayOfString')
    with open(bad_file_list) as f2:
        bad_files = f2.readlines()
        for file in bad_files:
            entities.item.append(file.strip())
    return entities

def generate_random_xndrc():

    xndrc_base=re.sub('[ ]+', ' ',
               '''LBONE_SERVER     dlt.incntre.iu.edu 6767
                  LBONE_SERVER     cup.eecs.utk.edu 6767
                  LOCATION         zip= 37938
                  DURATION 1d
                  STORAGE_TYPE     HARD
                  VERBOSE          1
                  MAX_INTERNAL_BUFFER  64M
                  DEMO 0
                  DATA_BLOCKSIZE 5M
                  E2E_BLOCKSIZE  512K
                  E2E_ORDER      none
                  COPIES         3
                  TIMEOUT     2600
                  THREADS  8
                  MAXDEPOTS 6''')

    with open('test-xndrc', 'w') as f:
        f.write(xndrc_base)
        depot, port = random.choice(DEPOTS)
        print(depot)
        f.write('\nDEPOT {} {}\n'.format(depot, port))


def fix_wsdl(wsdlurl):  # fix errors in USGS WSDL
    request = urllib2.urlopen(wsdlurl)
    tree = etree.parse(request)
#    tree = etree.parse(io.BytesIO(request.readall()))


    # add import soap-encoding to schema
    schemaNode = tree.xpath('/xmlns:definitions/xmlns:types/xsd:schema',
                            namespaces={'xmlns': 'http://schemas.xmlsoap.org/wsdl/',
                                        'xsd': 'http://www.w3.org/2001/XMLSchema'})[0]

    e = etree.Element('{http://www.w3.org/2001/XMLSchema}import',
                      namespace="http://schemas.xmlsoap.org/soap/encoding/",
                      nsmap={'xsd': 'http://www.w3.org/2001/XMLSchema'})

    schemaNode.insert(0, e)

    # Change the types of nodes with type xsd:struct to soap-end:Struct
    for node in tree.xpath('//*[@type="xsd:struct"]'):
        node.attrib["type"] = "soap-enc:Struct"
    # write the new wsdl to the local file system
    filepath = config['workspace'] + 'wsdl.xml'
    with open(filepath, 'wt') as out_file:
        out_file.write(etree.tostring(tree, method='html').decode())
        out_file.close()

    # return the filepath
    return filepath


def main():
    #instantiate global logger

    global logger
    logger = Logger()
    list_config()

    # fix bad wsdl file from USGS
    url = '''file://''' + fix_wsdl(wsdlurl=config['usgs_url'])  # Fix the wsdl URL
    # instantiate SOAP client

    try:
        client = Client(url)

    except:
        logger.write_error('Error instantiating SOAP client: ', sys.exc_into())

    # login - return apiKey
    if config['usgs_login'] is None:
        uname = input('Enter USGS Login: ')
    else:
        uname = config['usgs_login']
    if config['usgs_password'] is None:
        pword = getpass.getpass()
    else:
        pword = config['usgs_password']
    logger.write('loggin in...')
    try:
        apikey = client.service.login(uname, pword)
        assert isinstance(apikey, object)
        logger.write('Success: apiKey = ' + str(apikey))
    except Exception as e:
        logger.write('Login Failure: ' + e)
        exit(1)

    # instantiate a new instance of the helper class
    process = ProcessL8()

    # search for scenes and load scene[] with Scene objects:
    # instantiate a new Landsat_8 search object
    search = SearchL8()

    #  set attributes here that need to be changed
    search.set_attribs('client_obj', client)
    search.set_attribs('apikey', apikey)
    search.set_attribs('ll', process.get_service_class_coordinate(client=client, lat=config['ll'].split(',')[1],
                                                                  long=config['ll'].split(',')[0]))
    search.set_attribs('ur', process.get_service_class_coordinate(client=client, lat=config['ur'].split(',')[1],
                                                                  long=config['ur'].split(',')[0]))

    # set to start with record 0 on first pass
    search.set_attribs('nextRecord', 0)

    #search.set_attribs('criteria',['Cloud Cover = 4'])
    criteria = process.get_service_inventory_criteriafield(client=client, field='Cloud Cover', value=config['cloud'])

    entities_processed = 0
    files_processed = 0
    run = True
    while run is True:
        if config['bad_files']:
            entities = read_bad_file_list(client=client, bad_file_list=config['bad_file_list'])
            search.set_attribs('entities', entities)
            search.set_attribs('totalHits', len(entities.item))
            #print(search.get_attribs('entities'))
            #print(search.get_attribs('totalHits'))
        else:
            search.set_attribs('startingNumber', search.get_attribs('nextRecord'))
            for i in range(retry):  # retry 10 times then give up
                try:
                    response = search.do_search()
                except Exception as e:  # right now we are retrying on any error
                    # try logging in again
                    apikey = None
                    logger.write_error('Search_failed: ' + str(e))
                    apikey = client.service.login(uname, pword)
                    if apikey == None:
                        continue
                    logger.write_error('Success: apiKey = ' + str(apikey))
                break

        logger.write('Found ' + str(len(search.get_attribs('entities').item)) + ' scenes:')
        if config['bad_files']:
            logger.write('Processing bad file list')
        else:
            logger.write('Processing ' + str(search.get_attribs('firstRecord')) + ' - ' +
                        str(search.get_attribs('lastRecord')) + ' of ' + str(search.get_attribs('totalHits')) + ' matches')
        # if config['bad_files']:
        #
        # else:
        #     # get the list of available products (download_codes) for the selected scenes
        #     one_entity = client.factory.create('ArrayOfString')
        #     one_entity.item.append(search.get_attribs('entities').item[0])
        #     product_codes = process.get_product_codes(client=client, apikey=apikey, entity_ids=one_entity)
        #     #product    _codes = process.get_product_codes(client=client, apikey=apikey,
                                                  #entity_ids=search.get_attribs('entities'))

        logger.write('number found: ' + str(len(search.get_attribs('entities').item)))
        # cycle through the scenes\
        for entity in search.get_attribs('entities').item:
            logger.write('**Processing ' + entity + ' ...')

            if config['bad_files']:
                product_code = process.split_filename(entity=entity,part='product_code')
                #print('line 869 - product_code: '+product_code)
                entity = process.split_filename(entity=entity,part='entity_id')
                #print('line 872 - entity: '+entity)
                #print('entity: ', entity)
                #print('product_code: ', product_code)
            else:
                product_code = 'all'

            # need a single entity in an ArrayOfString to retrieve download Options
            entity_ids = client.factory.create('ArrayOfString')  # the SOAP server requires an ArrayOfString
            #entity_ids.__setitem__('item', entity)
            entity_ids.item.append(entity)

            logger.write('  Retrieving download options for ' + entity + ' ...')
            products = process.get_download_options(client=client, apikey=apikey, uname=uname, pword=pword,
                                                    entity_ids=entity_ids, product_code=product_code)

            logger.write('  Found ' + str(len(products)) + ' products')

            for product in products:
                # Make sure this product is available
                if args.avmss and product.get_attribs('download_code') != 'FR_BUND':
                    # skip everything but landsatLook bundles
                    logger.write('   skipping ' + product.get_attribs('download_code'))
                    continue

                logger.write('   -Product: ' + product.get_attribs('productName'))
                logger.write('       Product Code: ' + product.get_attribs('download_code'))
                if product.get_attribs('available'):
                    # even though we are sending only one product_id, the SOAP server requires an ArrayOfString
                    product_ids = client.factory.create('ArrayOfString')
                    product_ids.item.append(product.get_attribs('download_code'))

                    logger.write('       Retrieving URL for ' + entity + ' product: ' + product.get_attribs(
                        'download_code') + '...')
                    url = process.get_url(client=client, apikey=apikey, entity_ids=entity_ids, product_ids=product_ids,
                                          uname=uname, pword=pword)
                    if url is None:
                        logger.write_error('Failed to retrieve URL for ' + entity)
                        continue  # continue with the next entity
                    logger.write('       URL: ' + url)

                    # generate the various file paths
                    base_name = process.get_base_name(url=url)
                    file_name = process.get_file_name(base_name=base_name,
                                                      download_code=product.get_attribs('download_code'))
                    lodn_path = process.get_lodn_path(file_name)
                    file_path = config['workspace'] + file_name
                    xnd_path = file_path + '.xnd'
                    logger.write('       base_name: ' + base_name)
                    logger.write('       file_name: ' + file_name)
                    logger.write('       file_path: ' + file_path)
                    logger.write('       lodn_path: ' + lodn_path)
                    logger.write('       xnd_path: ' + xnd_path)

                    # check for existing exNode
                    # add checksumming or something here
                    if config['lodn_stat']:
                        skip = False
                        logger.write('       Looking for existing exnode for: ' + lodn_path)
                        if process.lodn_stat(lodn_path):
                            logger.write('       exNode exists, skipping file ...')
                            skip = True
                            continue  # skip the rest of this loop
                        else:
                            logger.write('       exNode not found - continue with download ...')

                    if config['file_download']:
                        # look for existing file on local storage
                        result = 0
                        if config['file_stat']:
                            logger.write('       Looking for ' + file_path)
                            if process.file_stat(file_path=file_path, file_size=product.get_attribs('file_size')):
                                logger.write('       ' + file_path + ' exists - skipping download')
                            else:
                                logger.write('       Not found - Downloading...')
                                result = process.download(url=url, file_path=file_path,
                                                          usgs_filesize=product.get_attribs('file_size'))
                                # if result != 0:
                                #     logger.write('******* Download Failed *******')
                                #     logger.write_error(file_path+' '+str(result))
                                #     continue  # go on to the next file
                        else:
                            logger.write('       Force downloading ' + url)
                            result = process.download(url=url, file_path=file_path,
                                                      usgs_filesize=product.get_attribs('file_size'))
                        if result != 0:
                            if url is None:
                                url = 'None'
                            if result is None:
                                result = 'None'
                            logger.write_error('Line 797: ******* Download Failed *******')
                            logger.write_error('URL: ' + url)
                            logger.write_error('Error: ' + result)
                            continue  # go on to the next file
                        files_processed = files_processed + 1

                    else:
                        logger.write('       Skipping download...')

                    print("finished download")

                    if config['lors_upload']:
                        # Upload the file using LoRS_upload - returns 0 if successful
                        logger.write('       Uploading ' + file_path + ' to EODN')
                        result = process.upload_to_eodn(xnd_file=xnd_path, file=file_path)
                        if result != 0:
                            logger.write_error('line 838: lors_upload failed for ' + base_name)
                            logger.write_error('result')
                    else:
                        logger.write('       Skipping LoRS upload...')

                    # Import the exNode to LoDN
                    if config['lodn_import']:
                        logger.write('       Building LoDN directory structure')
                        process.build_lodn_dir(product_id=base_name)
                        logger.write('       Success - built ' + lodn_path)
                        logger.write('       Importing ' + xnd_path + ' to LoDN')
                        result = process.import_exnode(xnd_path=xnd_path, lodn_path=lodn_path)
                        logger.write('       import_exnode results: ' + str(result))
                        if result != 0:
                            logger.write_error('Line 849: lodn_import failed for ' + base_name)
                            logger.write_error('Lodn_path = ' + lodn_path)
                            logger.write_error('result')
                    else:
                        logger.write('       Skipping LoDN import...')

                    if config['unis_import']:
                        logger.write('       Importing ' + file_name + ' to UNIS')
                        process.unis_import(file_name, xnd_path, base_name)
                    else:
                        logger.write('       Skipping UNIS import...')

                    # add scene to GloVis instance
                    if product.get_attribs('download_code') == 'STANDARD':
                        if config['glovis']:
                            logger.write('       Sending scene to GloVIS...')
                            result = process.add_to_glovis(product_id=base_name)
                            if result != 0:
                                logger.write_error('Line 857: GloVis copy failed for ' + base_name)
                                logger.write_error('result')
                        else:
                            logger.write('       Skipping sending scene to GloVis...')

                    # add scene to AVMSS instance
                    if product.get_attribs('download_code') == 'FR_BUND':
                        if config['avmss']:
                            logger.write('       Sending files to AVMSS...')
                            result = process.add_to_avmss(file_path=file_path, file_name=file_name)
                            if result != 0:
                                logger.write('Line 865: avmss copy failed for ' + base_name)
                                logger.write_error('result')
                        else:
                            logger.write('       Skipping sending files to AVMSS...')

                    # Remove working files
                    logger.write('       Cleaning up...')
                    if config['file_delete']:
                        try:
                            if os.path.isfile(file_path):
                                call(['rm', file_path])
                            if os.path.isfile(file_path):
                                call(['rm', xnd_path])
                        except Exception as e:
                            logger.write_error('Line 848: Clean up failed.')
                            logger.write_error(e)

                else:
                    logger.write('       ' + product.get_attribs('productName') + ' unavailable for ' + entity)

                logger.write('       Product complete.')
                # if skip == False:
                time.sleep(float(config['pause']))  # this probably isn't needed because of download delays
                # end if product available

            # end for product in products

            # end for entity in entities
            logger.write('    Finished with ' + entity)
            files_processed = files_processed + 1
            entities_processed = entities_processed + 1
            #logger.write('finished entity ' + str(entities_processes) + ' of ' + search.get_attribs('totalHits'))
            if entities_processed >= search.get_attribs('totalHits'):
                run = False

                # end while run

    logger.write('')
    logger.write('*** Processed ' + (str(files_processed)) + ' scenes')
    if downloads != 0:
        logger.write('   ' + str(downloads) + ' files downloaded at an average of ' + str(
            download_speeds / downloads) + ' kb/sec')

    # store the ending data as the starting date for next time around
    with open('harvest.cfg') as f1:
        lines = f1.readlines()

    with open('harvest.cfg', 'w') as f2:
        for line in lines:
            if line.startswith('start'):
                f2.write('start' + ' ' + str(search.get_attribs('end_date')) + '\n')
            else:
                f2.write(line)

    # logout before closing
    results = client.service.logout(apiKey=apikey)

    #logger.write('Logged out: ' + str(results))

    #logger.close()


if __name__ == '__main__': main()

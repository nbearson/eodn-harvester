import socket
import netifaces
import requests
import json

import eodnharvester.settings as settings
import eodnharvester.history as history

class UNISError(Exception): pass
class NoValueError(UNISError): pass
    

class HarvesterConfigure(object):
    def __init__(self, urn = None):
        self.log = history.GetLogger()
        self._cache = None
        self._dirty = False
        self._hostname = socket.gethostname()
        if urn:
            self._urn = urn
        else:
            self._urn = self._getUrn()
        
        self._node = self._setupNode()
        self._cache = self._createProxyService()
        
        if self._node:
            try:
                self._cache = self._getService()
            except requests.exceptions.HTTPError as exp:
                self.log.warn("Could not contact UNIS while getting service - {exp} - using defaults".format(exp = exp))
            except NoValueError as exp:
                self.log.warn("No service on record, using defaults")
                self._cache = self._createService()
                
    def edit(self, sid, config):
        if not self._node:
            self._node = self._setupNode()
            
        self._cache["properties"]["configurations"][sid] = config
        return self._update()
    
    def remove(self, sid):
        if not self._node:
            self._node = self._setupNode()
            
        del self._cache["properties"]["configurations"][sid]
        return self._update()
    
    def add(self, config):
        if not self._node:
            self._node = self._setupNode()
        
        self._cache["properties"]["configurations"].append(config)
        return self._update()
    
    def get(self):
        if self._dirty:
            self._update()
        
        try:
            if not self._dirty:
                self._cache = self._getService()
                self._update()
        except NoValueError as exp:
            self.log.warn("Could not find service - {exp}".format(exp = exp))
            if not self._node:
                self._node = self._setupNode()
                
            if self._node:
                try:
                    self._cache = self._getService()
                except requests.exceptions.HTTPError as exp:
                    self.log.warn("Could not contact UNIS while getting service - {exp} - using defaults".format(exp = exp))
                except NoValueError as exp:
                    self.log.warn("No service on record, using defaults")
                    self._cache = self._createService()
            
        except Exception as exp:
            self.log.warn("Error while getting service - {exp} - using cache".format(exp = exp))        
        finally:
            return self._cache["properties"]["configurations"]
    
    def _update(self):
        try:
            headers = { 'Content-Type': 'application/perfsonar+json' }
            url = "{protocol}://{host}:{port}/services/{sid}".format(protocol = "https" if settings.USE_SSL else "http",
                                                                     host = settings.UNIS_HOST,
                                                                     port = settings.UNIS_PORT,
                                                                     sid  = self._cache["id"])
            response = requests.put(url, data = json.dumps(self._cache), headers = headers, cert = (settings.SSL_OPTIONS["cert"], settings.SSL_OPTIONS["key"]))
        except requests.exceptions.RequestException as exp:
            error = "Failed to connect to UNIS - {exp}".format(exp = exp)
            self.log.error(error)
            self._dirty = True
            return False
        except Exception as exp:
            error = "Unkown error while contacting UNIS - {exp}".format(exp = exp)
            self.log.error(error)
            self._dirty = True
            return False

        self._dirty = False
        return True
    
    def _setupNode(self):
        try:
            return self._getNode()
        except NoValueError:
            self.log.warn("No node information in UNIS, creating new node")
            try:
                return self._createNode()
            except NoValueError as exp:
                self.log.error("Failed to create node on UNIS - {exp}".format(exp = exp))
            except Exception as exp:
                self.log.error("There was an unknown error while creating the node - {exp}".format(exp = exp))
        except requests.exceptions.HTTPError as exp:
            self.log.warn("Could not connect to UNIS server")
        except Exception as exp:
            self.log.error("There was an unknown error while contacting UNIS - {exp}".format(exp = exp))
            
        return None
        
    
    def _getNode(self):
        url = "{protocol}://{host}:{port}/nodes?urn={urn}".format(protocol = "https" if settings.USE_SSL else "http",
                                                                  host = settings.UNIS_HOST,
                                                                  port = settings.UNIS_PORT,
                                                                  urn  = self._urn)
        
        try:
            response = requests.get(url, cert = (settings.SSL_OPTIONS["cert"], settings.SSL_OPTIONS["key"]))
            response = response.json()
        except requests.exceptions.RequestException as exp:
            raise requests.exceptions.HTTPError(exp)
        except ValueError as exp:
            raise NoValueError("Invalid json encoding - {exp}".format(exp = exp))
        except Exception as exp:
            raise Exception(exp)
        
        if not response:
            raise NoValueError("No node found")
        
        return response[0]["selfRef"]
    
    def _createNode(self):
        url = "{protocol}://{host}:{port}/nodes".format(protocol = "https" if settings.USE_SSL else "http",
                                                        host = settings.UNIS_HOST,
                                                        port = settings.UNIS_PORT)
        
        node = {
            "name": self._hostname,
            "urn": self._urn,
        }
        try:
            response = requests.post(url, data = json.dumps(node), cert = (settings.SSL_OPTIONS["cert"], settings.SSL_OPTIONS["key"]))
            response = response.json()
        except requests.exceptions.RequestException as exp:
            raise requests.exceptions.HTTPError(exp)
        except ValueError as exp:
            raise NoValueError("Invalid json encoding - {exp}".format(exp = exp))
        except Exception as exp:
            raise Exception(exp)
       
        return response["selfRef"]
    
    def _getService(self):
        url = "{protocol}://{host}:{port}/services?serviceType=eodn:tools:harvester&runningOn.href={href}".format(protocol = "https" if settings.USE_SSL else "http",
                                                                                                                  host = settings.UNIS_HOST,
                                                                                                                  port = settings.UNIS_PORT,
                                                                                                                  href = self._node)
        try:
            response = requests.get(url, cert = (settings.SSL_OPTIONS["cert"], settings.SSL_OPTIONS["key"]))
            response = response.json()
        except requests.exceptions.RequestException as exp:
            raise requests.exceptions.HTTPError(exp)
        except ValueError as exp:
            raise NoValueError("Invalid json encoding - {exp}".format(exp = exp))
        except Exception as exp:
            raise Exception(exp)
        
        if not response:
            raise NoValueError("No service found")
        
        return response[0]
    
    
    def _createService(self):
        service = self._createProxyService()
        url = "{protocol}://{host}:{port}/services".format(protocol = "https" if settings.USE_SSL else "http",
                                                           host = settings.UNIS_HOST,
                                                           port = settings.UNIS_PORT)
        
        try:
            response = requests.post(url, data = json.dumps(service), cert = (settings.SSL_OPTIONS["cert"], settings.SSL_OPTIONS["key"]))
            response = response.json()
        except requests.exceptions.RequestException as exp:
            raise requests.exceptions.HTTPError(exp)
        except ValueError as exp:
            raise NoValueError("Invalid json encoding - {exp}".format(exp = exp))
        except Exception as exp:
            raise Exception(exp)
        
        return response
    
    def _createProxyService(self):
        service = {
            "status": "ON",
            "ttl": 900000,
            "serviceType": "eodn:tools:harvester",
            "description": "Configuration for a harvester pass",
            "properties": {
                "configurations": [self._getDefaultConfig()]
            },
            "name": "harvester",
            "runningOn": {
                "href": self._node,
                "rel": "full"
            }
        }
        
        return service
        
    def _getUrn(self):
        fqdn = socket.getfqdn()
        
        if not fqdn or not self._hostname:
            raise Exception("socket.getfqdn or socket.gethostname failed. Try settings urn manually")

        if fqdn != self._hostname:
            domain = fqdn.replace(self._hostname + ".", "")
            node = self._hosthost
        else:
            try:
                default_ip, default_iface = get_default_gateway_linux()
                default_ip  = netifaces.ifaddresses(default_iface)[netifaces.AF_INET][0]["addr"]
                default_mac = netifaces.ifaddresses(default_iface)[netifaces.AF_LINK][0]["addr"]
                default_mac = clean_mac(default_mac)
                domain = fqdn
                node = "{ip}_{mac}_{host}".format(ip = default_ip, mac = default_mac, host = self._hostname)
            except Exception as exp:
                domain = fqdn.replace(self._hostname + ".", "")
                node = self._hostname
                
        return "urn:ogf:network:domain={domain}:node={node}".format(domain = domain, node = node)
            
            

    def _getDefaultConfig(self):
        config = {}
        config["datasetName"] = settings.DATASET_NAME
        config["lowerLeft"]   = settings.LOWER_LEFT
        config["upperRight"]  = settings.UPPER_RIGHT
        config["sortOrder"]   = settings.SORT_ORDER
        config["node"]        = settings.NODE
        
        return config


def get_default_gateway_linux():
    """
    Reads the default gateway directly from /proc.
    Returns default interface ip, default interface name
    """
    with open("/proc/net/route") as fh:
        for line in fh:
            fields = line.strip().split()
            if fields[1] != '00000000' or not int(fields[3], 16) & 2:
                continue
            return socket.inet_ntoa(struct.pack("=L", int(fields[2], 16))),fields[0]


def clean_mac(mac):
    mac = mac.strip().lower().replace(":", "")
    mac = mac.replace(" ", "")
    try:
        return re.search('([0-9a-f]{12})', mac).groups()[0]
    except AttributeError:
        return None

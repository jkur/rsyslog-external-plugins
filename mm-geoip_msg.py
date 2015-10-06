#! /usr/bin/python

import sys
import json
import GeoIP
import re

# CONFIG
IPFIELD='src_ip'

# globals
geodb = None


def onInit():
    """ Do everything that is needed to initialize processing (e.g.
    open files, create handles, connect to systems...)
    """
    global geodb
    geodb = GeoIP.open("/usr/share/GeoIP/GeoIPCity.dat", GeoIP.GEOIP_STANDARD)
    #geodbv6 = GeoIP.open("/usr/share/GeoIP/GeoIPCity.dat", GeoIP.GEOIP_STANDARD)


def onReceive(msg):
    """This is the entry point where actual work needs to be done. It receives
    the messge from rsyslog and now needs to examine it, do any processing
    necessary. The to-be-modified properties (one or many) need to be pushed
    back to stdout, in JSON format, with no interim line breaks and a line
    break at the end of the JSON. If no field is to be modified, empty
    json ("{}") needs to be emitted.
    Note that no batching takes place (contrary to the output module skeleton)
    and so each message needs to be fully processed (rsyslog will wait for the
    reply before the next message is pushed to this module).
"""
    ret_msg = {}
    if IPFIELD in msg:
        r = re.search('"src_ip": "([0-9.:]+)"', msg)
        if r:
            ipaddr = r.groups()[0]
            geoip_result = geodb.record_by_addr(ipaddr)
            if geoip_result:
                for k,v in geoip_result.iteritems():
                    if k in ['latitude', 'longitude']:
                        continue
                    ret_msg.update({'{}_geoip.{}'.format(IPFIELD, k): v})
                ret_msg.update({'{}_geoip.coordinate'.format(IPFIELD): [geoip_result['longitude'], geoip_result['latitude']]})
            else:
                ret_msg.update({'{}_geoip.error'.format(IPFIELD): 'no entry found'})
    print json.dumps({'$!': ret_msg}, encoding="latin1")


def onExit():
     """ Do everything that is needed to finish processing (e.g.
     close files, handles, disconnect from systems...). This is
     being called immediately before exiting.
     """
     # most often, nothing to do here
     #geodb.close()





onInit()
keepRunning = 1
while keepRunning == 1:
    msg = sys.stdin.readline()
    if msg:
        msg = msg[:len(msg)-1] # remove LF
        onReceive(msg)
        sys.stdout.flush() # very important, Python buffers far too much!
    else: # an empty line means stdin has been closed
        keepRunning = 0
onExit()
sys.stdout.flush() # very important, Python buffers far too much!

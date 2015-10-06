#! /usr/bin/python

import sys
import json
import GeoIP

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
        jmsg = json.loads(msg)
        if jmsg.has_key('$!'):
                jmsg = jmsg['$!']
        ret_msg = {}
        #print jmsg['$!'].keys()
        if jmsg.has_key('src_ip'):
                geoip_result = geodb.record_by_addr(jmsg['src_ip'])
                if geoip_result:
                        for k,v in geoip_result.iteritems():
                                ret_msg.update({'src_geoip.{}'.format(k): v})

        if jmsg.has_key('dest_ip'):
                geoip_result = geodb.record_by_addr(jmsg['dest_ip'])
                if geoip_result:
                        for k,v in geoip_result.iteritems():
                                ret_msg.update({'src_geoip.{}'.format(k): v})
        print json.dumps({'$!': ret_msg}, encoding="latin1")


def onExit():
	""" Do everything that is needed to finish processing (e.g.
	    close files, handles, disconnect from systems...). This is
	    being called immediately before exiting.
	"""
	# most often, nothing to do here
        #geodb.close()

"""
-------------------------------------------------------
This is plumbing that DOES NOT need to be CHANGED
-------------------------------------------------------
Implementor's note: Python seems to very agressively
buffer stdouot. The end result was that rsyslog does not
receive the script's messages in a timely manner (sometimeseven never, probably due to races). To prevent this, we
flush stdout after we have done processing. This is especially
important once we get to the point where the plugin does
two-way conversations with rsyslog. Do NOT change this!
See also: https://github.com/rsyslog/rsyslog/issues/22
"""
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

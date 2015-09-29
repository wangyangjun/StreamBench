#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json

def update_broker_id(id):
	path = os.path.dirname(os.path.realpath(__file__))
	with open(path+'/kafka/config/server.properties') as infile, \
	open(path+'/kafka/config/server.properties.tmp', 'w') as outfile:
	    for line in infile:
	        if 'broker.id=' in line:
	        	line = 'broker.id='+str(id)+'\n'
	        outfile.write(line)

    	subprocess.call(['mv', path+'/kafka/config/server.properties.tmp', path+'/kafka/config/server.properties'])

def install_kafka(broker_id):
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'));

	# download kafka http://www.nic.funet.fi/pub/mirrors/apache.org/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz
	if 'kafka' not in subprocess.check_output(['ls']):
		p = subprocess.Popen(['wget', 'http://www.nic.funet.fi/pub/mirrors/apache.org/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'kafka_2.10-0.8.2.1.tgz'])
			p.wait()
			subprocess.call(['mv', 'kafka_2.10-0.8.2.1', 'kafka'])
	update_broker_id(broker_id)
	# cp kafka
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/kafka'])
	subprocess.call(['sudo', 'cp', '-r', 'kafka', '/usr/local/kafka'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/kafka'])
	# config
	subprocess.call(['rm', '-rf', '/usr/local/kafka/config'])
	subprocess.call(['cp', '-r', path+'/kafka/config', '/usr/local/kafka/config'])
	if '/mnt/kafka-logs' not in subprocess.check_output(['ls', '/mnt']):
		subprocess.call(['sudo', 'mkdir', '/mnt/kafka-logs'])
		subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/mnt/kafka-logs'])

if __name__ == "__main__":
	broker_id = 0
	if len(sys.argv) < 2:
		sys.stderr.write("Usage: python %s broker.id\n" % (sys.argv[0]))
		sys.exit(1)
	else:
		try:
			broker_id = int(sys.argv[1])
		except ValueError:
			sys.stderr.write("Usage: python %s broker.id\n" % (sys.argv[0]))
			sys.exit(1)
	install_kafka(broker_id)


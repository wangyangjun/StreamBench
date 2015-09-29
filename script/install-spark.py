#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json
from util import appendline

def install_storm():
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'));

	# download sprk http://mirror.netinch.com/pub/apache/spark/spark-1.5.0/spark-1.5.0-bin-hadoop2.6.tgz
	if 'spark' not in subprocess.check_output(['ls']):
		p = subprocess.Popen(['wget', 'http://mirror.netinch.com/pub/apache/spark/spark-1.5.0/spark-1.5.0-bin-hadoop2.6.tgz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'spark-1.5.0-bin-hadoop2.6.tgz'])
			p.wait()
			subprocess.call(['mv', 'spark-1.5.0-bin-hadoop2.6', 'spark'])
	# cp spark
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/spark'])
	subprocess.call(['sudo', 'cp', '-r', 'spark', '/usr/local/spark'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/spark'])
	# conf
	subprocess.call(['rm', '-rf', '/usr/local/spark/conf'])
	subprocess.call(['cp', '-r', path+'/spark/conf', '/usr/local/spark/conf'])
	
	# hosts
	for node in config['nodes']:
		appendline('/etc/hosts', node['ip']+'\t'+node['host'])


if __name__ == "__main__":
	install_storm()

	
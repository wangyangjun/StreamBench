#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json
from util import appendline

def install_flink():
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'));

	# download flink 
	if 'flink' not in subprocess.check_output(['ls']):
		p = subprocess.Popen(['wget', 'http://mirror.netinch.com/pub/apache/flink/flink-0.9.1/flink-0.9.1-bin-hadoop26.tgz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'flink-0.9.1-bin-hadoop26.tgz'])
			p.wait()
			subprocess.call(['mv', 'flink-0.9.1-bin-hadoop26', 'flink'])
	# cp flink
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/flink'])
	subprocess.call(['sudo', 'cp', '-r', 'flink', '/usr/local/flink'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/flink'])
	# conf
	subprocess.call(['rm', '-rf', '/usr/local/flink/conf'])
	subprocess.call(['cp', '-r', path+'/flink/conf', '/usr/local/flink/conf'])
	# mkdir flink tmp dir
	if 'flink' not in subprocess.check_output(['ls', '/mnt']):
		subprocess.call(['sudo', 'mkdir', '-p', '/mnt/flink'])
		if 'tmp' not in subprocess.check_output(['ls', '/mnt/flink']):
			subprocess.call(['sudo', 'mkdir', '-p', '/mnt/flink/tmp'])
		subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/mnt/flink'])

	# hosts
	for node in config['nodes']:
		appendline('/etc/hosts', node['ip']+'\t'+node['host'])


if __name__ == "__main__":
	install_flink()

	
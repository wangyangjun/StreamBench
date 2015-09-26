#!/bin/python
from __future__ import print_function
import subprocess
import sys
import json
from util import appendline, get_ip_address

def install_zookeeper():
	# /var/zookeeper/data chown
	config = json.load(open('zookeeper-config.json'));
	data_dir_maked = subprocess.check_call(["sudo", "mkdir", "-p", "/var/zookeeper/data"])
	if 0 == data_dir_maked:
		subprocess.call(["sudo", "chown", "-R", "cloud-user", "/var/zookeeper"])
	else:
		print("Create dirctory /var/zookeeper/data failed")
		sys.exist(1)
	print("Create dirctory /var/zookeeper/data successfully")
	# myid
	myip = get_ip_address()
	mynode = [node for node in config['nodes'] if node['ip'] == myip][0]
	open("/var/zookeeper/data/myid", "w").write(str(mynode['id']))
	print("Set myid for zookeeper successfully")
	# cp zookeeper
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/zookeeper'])
	subprocess.call(['sudo', 'cp', '-r', './zookeeper', '/usr/local/zookeeper'])
	for node in config['nodes']:
		appendline('/usr/local/zookeeper/conf/zoo.cfg', 'server.'+str(node['id'])+'=zoo'+str(node['id'])+':2888:3888')
	
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/zookeeper'])
	# hosts
	for node in config['nodes']:
		appendline('/etc/hosts', node['ip']+'\t'+'zoo'+str(node['id']))


if __name__ == "__main__":
	install_zookeeper()

	
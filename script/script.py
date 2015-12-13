#!/bin/python
from __future__ import print_function
import subprocess
import json
import os
from util import appendline

if __name__ == "__main__":
	config = json.load(open(os.path.dirname(os.path.realpath(__file__))+'/cluster-config.json'))
	# install git, clone repository, install jdk
	for node in config['nodes']:
		print("Install git on server %s" % node['ip'])
		# install git
		if '/usr/bin/git' not in subprocess.check_output(["ssh", "cloud-user@"+node['ip'], 'whereis git']):
			subprocess.call(["ssh", "cloud-user@"+node['ip'], "sudo apt-get install -y git"])
			print("Install git successfully")
		# clone repository
		files = subprocess.check_output(["ssh", "cloud-user@"+node['ip'], 'ls /home/cloud-user']).split('\n')
		if 'StreamBench' not in files:
			p = subprocess.Popen('ssh cloud-user@'+node['ip']+' "git clone https://github.com/wangyangjun/StreamBench.git"', shell=True)
		else:
			p = subprocess.Popen('ssh cloud-user@'+node['ip']+' "cd /home/cloud-user/StreamBench;git checkout .;git pull;"', shell=True)

		# install jdk
		if 0 == p.wait():
			# subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/StreamBench/script/install.py jdk6"])
			subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/StreamBench/script/install.py jdk7"])

		# install storm
		# if 0 == p.wait():
		# 	subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/StreamBench/script/install.py storm"])

		# install spark
		# if 0 == p.wait():
		# 	subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/StreamBench/script/install.py spark"])

		# install flink
		if 0 == p.wait():
			subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/StreamBench/script/install.py flink"])

		# install kafka
		if 0 == p.wait():
			subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/StreamBench/script/install.py kafka " + str(node['borker_id'])])

		# hosts
		subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/StreamBench/script/update-hosts.py"])

		# install hadoop
		# if 0 == p.wait():
		# 	subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/StreamBench/script/install.py hadoop"])
		
		
#!/bin/python
from __future__ import print_function
import subprocess
import json
import os

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
		files = subprocess.check_output(["ssh", "cloud-user@"+node['ip'], 'ls /home/cloud-user'])
		if 'RealtimeStreamBenchmark' not in files:
			p = subprocess.call(["ssh", "cloud-user@"+node['ip'], "git clone https://github.com/wangyangjun/RealtimeStreamBenchmark.git"])
		else:
			p = subprocess.Popen('ssh cloud-user@'+node['ip']+' "cd /home/cloud-user/RealtimeStreamBenchmark;git checkout .;git pull;"', shell=True)

		# install jdk
		if 0 == p.wait():
			subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/RealtimeStreamBenchmark/script/install-jdk.py"])

		# install storm
		# if 0 == p.wait():
		# 	subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/RealtimeStreamBenchmark/script/install-storm.py"])

		# install spark
		# if 0 == p.wait():
		# 	subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/RealtimeStreamBenchmark/script/install-spark.py"])

		# install flink
		# if 0 == p.wait():
		# 	subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/RealtimeStreamBenchmark/script/install-flink.py"])

		# install kafka
		if 0 == p.wait():
			subprocess.call(["ssh", "cloud-user@"+node['ip'], "python /home/cloud-user/RealtimeStreamBenchmark/script/install-kafka.py " + str(node['borker_id'])])

		# hosts
		appendline('/etc/hosts', '192.168.1.4	zoo1')
		appendline('/etc/hosts', '192.168.1.7	zoo2')
		appendline('/etc/hosts', '192.168.1.8	zoo3')
		appendline('/etc/hosts', '192.168.1.9	zoo4')
		appendline('/etc/hosts', '192.168.1.10	zoo5')


#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json
from util import appendline, get_ip_address


if __name__ == "__main__":
	# start server one by one
	servers = ['all', 'kafka', 'worker']
	if len(sys.argv) < 3 or sys.argv[1] not in servers:
		print('Usage: python %s " servers command"' % (sys.argv[0]))
		print('servers could be in "all", "kafka", "worker"')
		print("cd /home/cloud-user/StreamBench;git checkout .;git pull;")
		print("nohup bash /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties")
		print("nohup bash /usr/local/kafka/bin/kafka-server-stop.sh")
		print("nohup java -cp /home/cloud-user/StreamBench/jars/data-generator.jar fi.aalto.dmg.generator.WordCountDataGenerator &")
		sys.exit(1)
	else:
		path = os.path.dirname(os.path.realpath(__file__))
		config = json.load(open(path+'/cluster-config.json'))
		if sys.argv[1] == 'all':
			for node in config['nodes']:
				subprocess.Popen(['ssh', 'cloud-user@'+node['ip'], sys.argv[1]])
		elif sys.argv[1] == 'kafka':
			for node in config['nodes']:
				if node['kafka']:
					subprocess.Popen(['ssh', 'cloud-user@'+node['ip'], sys.argv[1]])
		elif sys.argv[1] == 'worker':
			for node in config['nodes']:
				if node['kafka'] == False:
					subprocess.Popen(['ssh', 'cloud-user@'+node['ip'], sys.argv[1]])
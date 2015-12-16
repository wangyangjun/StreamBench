#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json
from util import appendline, get_ip_address


if __name__ == "__main__":
	# start server one by one
	if len(sys.argv) < 2:
		print('Usage: python %s "command"\n' % (sys.argv[0]))
		print("cd /home/cloud-user/StreamBench;git checkout .;git pull;")
		print("nohup bash /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties")
		print("nohup bash /usr/local/kafka/bin/kafka-server-stop.sh")
		print("nohup java -cp /home/cloud-user/StreamBench/jars/data-generator.jar fi.aalto.dmg.generator.WordCountDataGenerator &")
		sys.exit(1)
	else:
		path = os.path.dirname(os.path.realpath(__file__))
		config = json.load(open(path+'/cluster-config.json'))
		for node in config['nodes']:
			subprocess.Popen(['ssh', 'cloud-user@'+node['ip'], sys.argv[1]])
		

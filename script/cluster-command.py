#!/bin/python
from __future__ import print_function
import subprocess
import sys
import json
from util import appendline, get_ip_address


if __name__ == "__main__":
	# start server one by one
	if len(sys.argv) < 2:
		sys.stderr.write('Usage: python %s "command"\n' % (sys.argv[0]))
		sys.stderr.write("cd /home/cloud-user/StreamBench;git checkout .;git pull;")
		sys.stderr.write("nohup bash /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties")
		sys.stderr.write("nohup bash /usr/local/kafka/bin/kafka-server-stop.sh")
		sys.stderr.write("nohup java -cp /home/cloud-user/StreamBench/jars/data-generator.jar fi.aalto.dmg.generator.WordCountDataGenerator &")
		sys.exit(1)
	else:
		path = os.path.dirname(os.path.realpath(__file__))
		config = json.load(open(path+'/cluster-config.json'))
		for node in config['nodes']:
			subprocess.Popen(['ssh', 'cloud-user@'+node['ip'], argv[1]])
		

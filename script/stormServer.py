#!/bin/python
from __future__ import print_function
import subprocess
import sys
import json
from util import appendline, get_ip_address


if __name__ == "__main__":
	# start server one by one
	if len(sys.argv) < 2 or sys.argv[1] not in ['start', 'stop']:
		sys.stderr.write("Usage: python %s start or stop\n" % (sys.argv[0]))
		sys.exit(1)
	else:
		config = json.load(open('cluster-config.json'));
		if sys.argv[1] == 'start':
			for node in config['nodes']:
				if node['master']:
					subprocess.call(['ssh', 'cloud-user@'+node['ip'], 'nohup /usr/local/storm/bin/storm nimbus&'])
					subprocess.call(['ssh', 'cloud-user@'+node['ip'], 'nohup /usr/local/storm/bin/storm ui&'])
				else:
					subprocess.call(['ssh', 'cloud-user@'+node['ip'], 'nohup /usr/local/storm/bin/storm subvisor&'])
					
		else:
			output = subprocess.check_output(['jps'])
			for process in output.split('\n'):
				if 'supervisor' in process or 'nimbus' in process:
					pid = process.split(' ')[0]
					# kill pid
					subprocess.call(['ssh', 'cloud-user@'+node['ip'], 'kill -9 ' + pid])

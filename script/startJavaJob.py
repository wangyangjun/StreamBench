#!/bin/python
from __future__ import print_function
import subprocess
import sys
import json
from util import appendline, get_ip_address


if __name__ == "__main__":
	# start server one by one
	if len(sys.argv) < 3:
		sys.stderr.write("Usage: python %s jar-file class-path\n" % (sys.argv[0]))
		sys.exit(1)
	else:
		config = json.load(open('cluster-config.json'))
		for node in config['nodes']:
			subprocess.Popen(['ssh', 'cloud-user@'+node['ip'], 'nohup java -cp ' + sys.argv[1] + ' ' + sys.argv[2] + '&'])

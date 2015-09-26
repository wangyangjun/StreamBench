#!/bin/python
from __future__ import print_function
import subprocess
import sys
import json
from util import appendline, get_ip_address


if __name__ == "__main__":
	if len(sys.argv) < 2 or sys.argv[1] not in ['start', 'stop']:
		sys.stderr.write("Usage: python %s start or stop\n" % (sys.argv[0]))
		sys.exit(1)
	else:
		config = json.load(open('zookeeper-config.json'));
		for node in config['nodes']:
			subprocess.call(['ssh', 'cloud-user@'+node['ip'], 'bash /usr/local/zookeeper/bin/zkServer.sh ' + sys.argv[1]])

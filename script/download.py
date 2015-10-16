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
		print("Download logs on server %s" % node['ip'])
		subprocess.call(["scp", "-r", "cloud-user@"+node['ip']+":/mnt/anis-logs", "~/2.0/"+node['host']+'-anis-logs'])

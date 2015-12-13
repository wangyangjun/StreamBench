#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json
from util import appendline, get_ip_address

if __name__ == "__main__":
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'));
	for node in config['nodes']:
		p = subprocess.Popen('ssh cloud-user@'+node['ip']+' "cd /home/cloud-user/RealtimeStreamBenchmark;git checkout .;git pull;"', shell=True)


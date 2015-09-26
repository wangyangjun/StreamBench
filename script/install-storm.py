#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json
from util import appendline

def install_storm():
	config = json.load(open(os.path.dirname(os.path.realpath(__file__))+'/cluster-config.json'));

	# cp storm
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/storm'])
	subprocess.call(['sudo', 'cp', '-r', './storm', '/usr/local/storm'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/storm'])
	# hosts
	for node in config['nodes']:
		appendline('/etc/hosts', node['ip']+'\t'+node['host'])


if __name__ == "__main__":
	install_storm()

	
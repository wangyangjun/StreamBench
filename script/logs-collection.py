#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json

storm_ports = ['6700', '6701', '6702', '6703']
if __name__ == "__main__":
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'))
	platforms = ['flink', 'storm', 'spark']
	if len(sys.argv) < 3 or sys.argv[1] not in platforms:
		print('Usage: python %s platform output.tar' % (sys.argv[0]))
		sys.exit(1)
	if 'flink' == sys.argv[1]:	
		subprocess.Popen('mkdir tmplogs', shell=True).wait()
		for node in config['nodes']:
			if node['kafka'] != True:
				subprocess.Popen('scp ' + node['host'] + ':/usr/local/flink/log/*.log tmplogs/', shell=True).wait()
		subprocess.Popen('tar cvf ' + sys.argv[2] + ' tmplogs/*', shell=True).wait()
		subprocess.Popen('rm -rf tmplogs', shell=True)
	elif 'storm' == sys.argv[1]:
		subprocess.Popen('mkdir tmplogs', shell=True).wait()
		for node in config['nodes']:
			if node['kafka'] != True:
				for port in storm_ports:
					subprocess.Popen('scp ' + node['host'] + ':/usr/local/storm/logs/*'+port+'.log ' \
						+ 'tmplogs/' + node['host']+port+'.log', shell=True).wait()
		subprocess.Popen('tar cvf ' + sys.argv[2] + ' tmplogs/*', shell=True).wait()
		subprocess.Popen('rm -rf tmplogs', shell=True).wait()
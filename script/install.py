#!/bin/python
from __future__ import print_function
import subprocess
import sys
from util import appendline
from installlib import *

softwares = ['jdk7', 'zookeeper', 'storm', 'spark', 'flink', 'kafka', 'hadoop']

if __name__ == "__main__":

	if len(sys.argv) < 2 or sys.argv[1] not in softwares:
		sys.stderr.write("Usage: python %s software name %s \n" % (sys.argv[0], softwares))
		sys.exit(1)
	else:
		print("Start install %s \n" % sys.argv[1])
		if 'jdk7' == sys.argv[1]:
			install_jdk7()
		elif 'zookeeper' == sys.argv[1]:
			install_zookeeper()
		elif 'storm' == sys.argv[1]:
			install_storm()
		elif 'spark' == sys.argv[1]:
			install_spark()
		elif 'flink' == sys.argv[1]:
			install_flink()
		elif 'kafka' == sys.argv[1]:
			broker_id = 0
			if len(sys.argv) < 3:
				sys.stderr.write("Usage: python %s kafka broker.id\n" % (sys.argv[0]))
				sys.exit(1)
			else:
				try:
					broker_id = int(sys.argv[2])
				except ValueError:
					sys.stderr.write("Usage: python %s kafka broker.id\n" % (sys.argv[0]))
					sys.exit(1)
			install_kafka(broker_id)
		elif 'hadoop' == sys.argv[1]:
			install_hadoop()
			
	
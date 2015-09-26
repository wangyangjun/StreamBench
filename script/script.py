#!/bin/python
from __future__ import print_function
import subprocess

if __name__ == "__main__":
	config = json.load(open('cluster-config.json'))
	# install git, clone repository, install jdk
	for node in config['nodes']:
		# install git
		if '/usr/bin/git' not in subprocess.check_output(['whereis', 'git']):
			subprocess.call(["sudo", "apt-get", "install", "-y", "git"])
			print("Install git")
		# clone repository
		files = subprocess.check_output(['ls','/home/cloud-user'])
		if 'RealtimeStreamBenchmark' not in files:
			subprocess.call(["git", "clone", "https://github.com/wangyangjun/RealtimeStreamBenchmark.git"])
		else:
			subprocess.Popen("git pull", shell=True, cwd="/home/cloud-user/RealtimeStreamBenchmark")
		# install jdk
		subprocess.call(["python", "/home/cloud-user/RealtimeStreamBenchmark/script/install-jdk.py"])

		# install storm
		subprocess.call(["python", "/home/cloud-user/RealtimeStreamBenchmark/script/install-storm.py"])


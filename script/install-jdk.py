#!/bin/python
from __future__ import print_function
import subprocess
from util import appendline

def install_JDK():
	# Install JDK 1.6
	if 'java-1.6.0-openjdk-amd64' not in subprocess.check_output(['ls', '/usr/lib/jvm']):
		update = subprocess.Popen(["sudo", "apt-get", "update"])
		if update.wait() == 0:
			jdk_installed = subprocess.check_call(["sudo", "apt-get", "install", "-y", "openjdk-6-jdk"])
			if jdk_installed == 0:
				print("JDK 1.6 installed successfully")
		else:
			print("apt-get update failed on server")
	else:
		print("JDK 1.6 is already installed.")

	if 'java-1.7.0-openjdk-amd64' not in subprocess.check_output(['ls', '/usr/lib/jvm']):
		update = subprocess.Popen(["sudo", "apt-get", "update"])
		if update.wait() == 0:
			jdk_installed = subprocess.check_call(["sudo", "apt-get", "install", "-y", "openjdk-7-jdk"])
			if jdk_installed == 0:
				print("JDK 1.7 installed successfully")
		else:
			print("apt-get update failed on server")
	else:
		print("JDK 1.7 is already installed.")

	# set JAVA_HOME 1.6
	appendline('/etc/profile', 'export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-amd64/')
	appendline('/etc/profile', 'export PATH=$JAVA_HOME/bin:$PATH')

if __name__ == "__main__":
	# install jdk 6
	install_JDK()
	
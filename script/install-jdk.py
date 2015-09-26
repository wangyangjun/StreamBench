#!/bin/python
from __future__ import print_function
import subprocess
from util import appendline

def install_JDK():
	if '/usr/bin/java' not in subprocess.check_output(['whereis', 'java']):
		jdk_installed = subprocess.check_call(["sudo", "apt-get", "install", "-y", "openjdk-6-jdk"])
		if jdk_installed == 0:
			print("JDK installed successfully")
	else:
		print("JDK is already installed.")
	# set JAVA_HOME
	appendline('/etc/profile', 'export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-amd64/')
	appendline('/etc/profile', 'export PATH=$JAVA_HOME/bin:$PATH')

if __name__ == "__main__":
	# install jdk 6
	install_JDK()
	
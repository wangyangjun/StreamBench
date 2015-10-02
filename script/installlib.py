#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json
from util import appendline, get_ip_address

def install_jdk6():
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
	# set JAVA_HOME 1.6
	appendline('/etc/profile', 'export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-amd64/')
	appendline('/etc/profile', 'export PATH=$JAVA_HOME/bin:$PATH')

def install_jdk7():
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

def install_zookeeper():
	# /var/zookeeper/data chown
	config = json.load(open('zookeeper-config.json'));
	data_dir_maked = subprocess.check_call(["sudo", "mkdir", "-p", "/var/zookeeper/data"])
	if 0 == data_dir_maked:
		subprocess.call(["sudo", "chown", "-R", "cloud-user", "/var/zookeeper"])
	else:
		print("Create dirctory /var/zookeeper/data failed")
		sys.exist(1)
	print("Create dirctory /var/zookeeper/data successfully")
	# myid
	myip = get_ip_address()
	mynode = [node for node in config['nodes'] if node['ip'] == myip][0]
	open("/var/zookeeper/data/myid", "w").write(str(mynode['id']))
	print("Set myid for zookeeper successfully")
	# cp zookeeper
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/zookeeper'])
	subprocess.call(['sudo', 'cp', '-r', './zookeeper', '/usr/local/zookeeper'])
	for node in config['nodes']:
		appendline('/usr/local/zookeeper/conf/zoo.cfg', 'server.'+str(node['id'])+'=zoo'+str(node['id'])+':2888:3888')
	
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/zookeeper'])
	# hosts
	for node in config['nodes']:
		appendline('/etc/hosts', node['ip']+'\t'+'zoo'+str(node['id']))

def install_storm():
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'));

	# cp storm
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/storm'])
	subprocess.call(['sudo', 'cp', '-r', path+'/storm', '/usr/local/storm'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/storm'])
	# hosts
	for node in config['nodes']:
		appendline('/etc/hosts', node['ip']+'\t'+node['host'])

def install_spark():
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'));

	# download sprk http://mirror.netinch.com/pub/apache/spark/spark-1.5.0/spark-1.5.0-bin-hadoop2.6.tgz
	if 'spark' not in subprocess.check_output(['ls']):
		p = subprocess.Popen(['wget', 'http://mirror.netinch.com/pub/apache/spark/spark-1.5.0/spark-1.5.0-bin-hadoop2.6.tgz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'spark-1.5.0-bin-hadoop2.6.tgz'])
			p.wait()
			subprocess.call(['mv', 'spark-1.5.0-bin-hadoop2.6', 'spark'])
	# cp spark
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/spark'])
	subprocess.call(['sudo', 'cp', '-r', 'spark', '/usr/local/spark'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/spark'])
	# conf
	subprocess.call(['rm', '-rf', '/usr/local/spark/conf'])
	subprocess.call(['cp', '-r', path+'/spark/conf', '/usr/local/spark/conf'])
	

def install_flink():
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'));

	# download flink 
	if 'flink' not in subprocess.check_output(['ls']):
		p = subprocess.Popen(['wget', 'http://mirror.netinch.com/pub/apache/flink/flink-0.9.1/flink-0.9.1-bin-hadoop26.tgz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'flink-0.9.1-bin-hadoop26.tgz'])
			p.wait()
			subprocess.call(['mv', 'flink-0.9.1', 'flink'])
	# cp flink
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/flink'])
	subprocess.call(['sudo', 'cp', '-r', 'flink', '/usr/local/flink'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/flink'])
	# conf
	subprocess.call(['rm', '-rf', '/usr/local/flink/conf'])
	subprocess.call(['cp', '-r', path+'/flink/conf', '/usr/local/flink/conf'])
	# mkdir flink tmp dir
	if 'flink' not in subprocess.check_output(['ls', '/mnt']):
		subprocess.call(['sudo', 'mkdir', '-p', '/mnt/flink'])
		if 'tmp' not in subprocess.check_output(['ls', '/mnt/flink']):
			subprocess.call(['sudo', 'mkdir', '-p', '/mnt/flink/tmp'])
		subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/mnt/flink'])

def install_hadoop():
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'))
	if 'hadoop' not in subprocess.check_output(['ls']).split('\n'):
		p = subprocess.Popen(['wget', 'http://mirror.netinch.com/pub/apache/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'hadoop-2.6.0.tar.gz'])
			p.wait()
			subprocess.call(['mv', 'hadoop-2.6.0', 'hadoop'])
	# cp hadoop
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/hadoop'])
	subprocess.call(['sudo', 'cp', '-r', 'hadoop', '/usr/local/hadoop'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/hadoop'])
	# conf
	subprocess.call(['rm', '-rf', '/usr/local/hadoop/etc'])
	subprocess.call(['cp', '-r', path+'/hadoop/etc', '/usr/local/hadoop/etc'])
	# mkdir hadoop tmp dir
	if 'hadoop' not in subprocess.check_output(['ls', '/mnt']):
		subprocess.call(['sudo', 'mkdir', '-p', '/mnt/hadoop'])
		if 'namenode' not in subprocess.check_output(['ls', '/mnt/hadoop']):
			subprocess.call(['sudo', 'mkdir', '-p', '/mnt/hadoop/namenode'])
		if 'datanode' not in subprocess.check_output(['ls', '/mnt/hadoop']):
			subprocess.call(['sudo', 'mkdir', '-p', '/mnt/hadoop/datanode'])
		subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/mnt/hadoop'])


def update_broker_id(id):
	path = os.path.dirname(os.path.realpath(__file__))
	with open(path+'/kafka/config/server.properties') as infile, \
	open(path+'/kafka/config/server.properties.tmp', 'w') as outfile:
	    for line in infile:
	        if 'broker.id=' in line:
	        	line = 'broker.id='+str(id)+'\n'
	        outfile.write(line)

    	subprocess.call(['mv', path+'/kafka/config/server.properties.tmp', path+'/kafka/config/server.properties'])

def install_kafka(broker_id):
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'));

	# download kafka http://www.nic.funet.fi/pub/mirrors/apache.org/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz
	if 'kafka' not in subprocess.check_output(['ls']):
		p = subprocess.Popen(['wget', 'http://www.nic.funet.fi/pub/mirrors/apache.org/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'kafka_2.10-0.8.2.1.tgz'])
			p.wait()
			subprocess.call(['mv', 'kafka_2.10-0.8.2.1', 'kafka'])
	update_broker_id(broker_id)
	# cp kafka
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/kafka'])
	subprocess.call(['sudo', 'cp', '-r', 'kafka', '/usr/local/kafka'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/kafka'])
	# config
	subprocess.call(['rm', '-rf', '/usr/local/kafka/config'])
	subprocess.call(['cp', '-r', path+'/kafka/config', '/usr/local/kafka/config'])
	# remove modification in config
	subprocess.call('cd ' + path + '; git checkout .', shell=True)
	if 'kafka-logs' not in subprocess.check_output(['ls', '/mnt']):
		subprocess.call(['sudo', 'mkdir', '/mnt/kafka-logs'])
		subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/mnt/kafka-logs'])


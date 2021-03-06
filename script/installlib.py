#!/bin/python
from __future__ import print_function
import subprocess
import sys
import os
import json
from util import appendline, get_ip_address
	
def install_jdk7():
	if  'jvm' not in subprocess.check_output(['ls', '/usr/lib']).split('\n') \
	 or 'java-1.7.0-openjdk-amd64' not in subprocess.check_output(['ls', '/usr/lib/jvm']).split('\n'):
		update = subprocess.Popen(["sudo", "apt-get", "update"])
		if update.wait() == 0:
			jdk_installed = subprocess.check_call(["sudo", "apt-get", "install", "-y", "openjdk-7-jdk"])
			if jdk_installed == 0:
				print("JDK 1.7 installed successfully")
		else:
			print("apt-get update failed on server")
	else:
		print("JDK 1.7 is already installed.")
	appendline('/home/cloud-user/.profile', 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64/')
	appendline('/home/cloud-user/.profile', 'export PATH=$JAVA_HOME/bin:$PATH')

def install_zookeeper():
	# /mnt/zookeeper/data chown
	config = json.load(open('zookeeper-config.json'));
	data_dir_maked = subprocess.check_call(["sudo", "mkdir", "-p", "/mnt/zookeeper/data"])
	if 0 == data_dir_maked:
		subprocess.call(["sudo", "chown", "-R", "cloud-user", "/mnt/zookeeper"])
	else:
		print("Create dirctory /mnt/zookeeper/data failed")
		sys.exist(1)
	print("Create dirctory /mnt/zookeeper/data successfully")
	# myid
	myip = get_ip_address()
	mynode = [node for node in config['nodes'] if node['ip'] == myip][0]
	open("/mnt/zookeeper/data/myid", "w").write(str(mynode['id']))
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

	# download storm 
	if 'storm' not in subprocess.check_output(['ls']).split('\n'):
		p = subprocess.Popen(['wget', 'http://mirror.netinch.com/pub/apache/storm/apache-storm-0.10.0/apache-storm-0.10.0.tar.gz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'apache-storm-0.10.0.tar.gz'])
			p.wait()
			subprocess.call(['mv', 'apache-storm-0.10.0', 'storm'])

	# cp storm
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/storm'])
	subprocess.call(['sudo', 'cp', '-r', 'storm', '/usr/local/storm'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/storm'])

	# conf
	subprocess.call(['rm', '-rf', '/usr/local/storm/conf'])
	subprocess.call(['cp', '-r', path+'/storm/conf', '/usr/local/storm/conf'])
	subprocess.call(['rm', '-rf', '/usr/local/storm/log4j2'])
	subprocess.call(['cp', '-r', path+'/storm/log4j2', '/usr/local/storm/log4j2'])


def install_spark():
	path = os.path.dirname(os.path.realpath(__file__))
	config = json.load(open(path+'/cluster-config.json'));

	# download sprk http://mirror.netinch.com/pub/apache/spark/spark-1.5.1/spark-1.5.1-bin-hadoop2.6.tgz
	if 'spark' not in subprocess.check_output(['ls']).split('\n'):
		p = subprocess.Popen(['wget', 'http://mirror.netinch.com/pub/apache/spark/spark-1.5.1/spark-1.5.1-bin-hadoop2.6.tgz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'spark-1.5.1-bin-hadoop2.6.tgz'])
			p.wait()
			subprocess.call(['mv', 'spark-1.5.1-bin-hadoop2.6', 'spark'])
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
	if 'flink' not in subprocess.check_output(['ls']).split('\n'):
		p = subprocess.Popen(['wget', 'http://mirror.netinch.com/pub/apache/flink/flink-0.10.1/flink-0.10.1-bin-hadoop26-scala_2.10.tgz'])
		if 0 == p.wait():
			# exact
			p = subprocess.Popen(['tar', '-zvxf', 'flink-0.10.1-bin-hadoop26-scala_2.10.tgz'])
			p.wait()
			subprocess.call(['mv', 'flink-0.10.1', 'flink'])
	# cp flink
	subprocess.call(['sudo', 'rm', '-rf', '/usr/local/flink'])
	subprocess.call(['sudo', 'cp', '-r', 'flink', '/usr/local/flink'])
	subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/usr/local/flink'])
	# conf
	subprocess.call(['rm', '-rf', '/usr/local/flink/conf'])
	subprocess.call(['cp', '-r', path+'/flink/conf', '/usr/local/flink/conf'])
	# mkdir flink tmp dir
	if 'flink' not in subprocess.check_output(['ls', '/mnt']).split('\n'):
		subprocess.call(['sudo', 'mkdir', '-p', '/mnt/flink'])
		if 'tmp' not in subprocess.check_output(['ls', '/mnt/flink']).split('\n'):
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
	if 'hadoop' not in subprocess.check_output(['ls', '/mnt']).split('\n'):
		subprocess.call(['sudo', 'mkdir', '-p', '/mnt/hadoop'])
		if 'namenode' not in subprocess.check_output(['ls', '/mnt/hadoop']).split('\n'):
			if 0 == subprocess.call(['sudo', 'mkdir', '-p', '/mnt/hadoop/namenode']):
				print("/mnt/hadoop/namenode created successfully")
		if 'datanode' not in subprocess.check_output(['ls', '/mnt/hadoop']).split('\n'):
			if 0 == subprocess.call(['sudo', 'mkdir', '-p', '/mnt/hadoop/datanode']):
				print("/mnt/hadoop/datanode created successfully")
		subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/mnt/hadoop'])
	# set hadoop home
	appendline('/home/cloud-user/.profile', 'export HADOOP_HOME=/usr/local/hadoop/')
	appendline('/home/cloud-user/.profile', 'export PATH=$HADOOP_HOME/bin:$PATH')


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
	if 'kafka' not in subprocess.check_output(['ls']).split('\n'):
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
	if 'kafka' not in subprocess.check_output(['ls', '/mnt']).split('\n'):
		subprocess.call(['sudo', 'mkdir', '/mnt/kafka'])
		subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/mnt/kafka'])

		if 'logs' not in subprocess.check_output(['ls', '/mnt/kafka']).split('\n'):
			subprocess.call(['sudo', 'mkdir', '/mnt/kafka/logs'])
			subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/mnt/kafka/logs'])
		if 'data' not in subprocess.check_output(['ls', '/mnt/kafka']).split('\n'):
			subprocess.call(['sudo', 'mkdir', '/mnt/kafka/data'])
			subprocess.call(['sudo', 'chown', '-R', 'cloud-user', '/mnt/kafka/data'])



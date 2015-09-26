#!/bin/python
from __future__ import print_function
import sys
import subprocess


############################################################################
# Set ssh login without password for nodes in the experiment cluster
# Usage: python set-ssh-keypass.py ip1 ip2 ...
# It is used in cPouta VM of csc.fi
############################################################################

# scp -i .ssh/csc.key -o StrictHostKeyChecking=no .ssh/id_rsa* cloud-user@192.168.1.7:~/.ssh/
# ssh -i .ssh/csc.key cloud-user@192.168.1.7
# cat .ssh/id_rsa.pub >> .ssh/authorized_keys

# remote_ips = ['192.168.1.7', '192.168.1.8', '192.168.1.9', '192.168.1.10']
remote_ips =[]

if __name__ == '__main__':
	if len(sys.argv) < 2:
		sys.stderr.write("Usage: python %s ip1 ip2 ...\n" % (sys.argv[0]))
		sys.exit(1)
	else:
		for arg in sys.argv[1:]:
			remote_ips.append(arg)
	public_key = open('/home/cloud-user/.ssh/id_rsa.pub').read()
	for ip in remote_ips:
		print("Configure ssh for server %s" % ip)
		# Check whether could connect to this remote server successfully or not
		if 0 == subprocess.call(['ssh', '-i', '/home/cloud-user/.ssh/csc.key', \
			'-o', 'StrictHostKeyChecking=no', 'cloud-user@'+ip, 'pwd']):
			subprocess.call(['scp', '-i', '/home/cloud-user/.ssh/csc.key', \
		 		'/home/cloud-user/.ssh/id_rsa.pub', 'cloud-user@'+ip+':/home/cloud-user/.ssh/'])
			subprocess.call(['scp', '-i', '/home/cloud-user/.ssh/csc.key', \
				'/home/cloud-user/.ssh/id_rsa', 'cloud-user@'+ip+':/home/cloud-user/.ssh/'])
			# In remote server, if public key doesn't exist in authorized_keys, then add it
		
			if public_key not in subprocess.check_output(['ssh', '-i', '/home/cloud-user/.ssh/csc.key', \
					'cloud-user@'+ip, 'cat /home/cloud-user/.ssh/authorized_keys']):
				subprocess.call(['ssh', '-i', '/home/cloud-user/.ssh/csc.key', \
					'cloud-user@'+ip, 'cat /home/cloud-user/.ssh/id_rsa.pub >> /home/cloud-user/.ssh/authorized_keys'])
			else:
				print("Public key is already in the authorized_keys of remote server.")
		else:
			print("The server %s couldn't be connected" % ip)
	sys.exit(0)


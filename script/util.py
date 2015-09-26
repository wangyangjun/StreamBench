from __future__ import print_function
import subprocess
import socket

# append line into file, if line doesn't exist in file
def appendline(filename, line):
	with open(filename, 'rt') as f:
		f_content = f.read()
		if line not in f_content:
			s = f_content + line + '\n'
			with open('/tmp/temporary', 'wt') as outf:
				outf.write(s)
			fileUpdated = subprocess.check_call(["sudo", "mv", "/tmp/temporary", filename])
			if fileUpdated == 0:
				subprocess.call(["echo", filename + " updated successfully"])
			else:
				subprocess.call(["echo", filename + " updated failed"])
		else:
			print('"%s" is already in file %s' % line, filename)


def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]
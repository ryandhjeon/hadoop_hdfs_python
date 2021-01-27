#!/usr/bin/python

# @author rdyer

from hdfs import InsecureClient
from datetime import datetime

client = InsecureClient(url='http://localhost:9870', root='/')

# helper to print file permissions in readable format
# e.g., -rwxr-xr-x or drwxr-xrwx
def perms(p, prefix='-'):
	s = ''
	for idx in range(0, 3):
		if int(p[idx]) & 4 is 4: s = s + 'r'
		else: s = s + '-'
		if int(p[idx]) & 2 is 2: s = s + 'w'
		else: s = s + '-'
		if int(p[idx]) & 1 is 1: s = s + 'x'
		else: s = s + '-'

	return prefix + s

# helper to print stats about a file/directory
# the prefix is the first printed permission,
# a 'd' for directory otherwise '-'
def printfile(name, stats, prefix='-'):
	print(' '.join((\
		perms(stats['permission'], prefix),\
		'  -' if stats['replication'] is 0 else '%3d' % stats['replication'],\
		stats['owner'],\
		stats['group'],\
		'%10d' % stats['length'],\
		datetime.fromtimestamp(stats['modificationTime'] / 1000).strftime('%Y-%m-%d %H:%M'),\
		name)))


# 1. Make a directory named: /activity1/
content = client.content('/activity1')
print(content)

# client.makedirs(hdfs_path='/activity1/', permission=None)
# client.makedirs(hdfs_path='/activity1/data/', permission=None)
# print(client.list(hdfs_path='/'))
# print('/activity1/data directory created')

# 2. Put the file RandomText.txt into HDFS as the path: /activity1/data/RandomText.txt
# client.upload(hdfs_path='/activity1/data/', local_path='/Workspace/cs6500_sp2021_r02_jeon/RandomText.txt')
# print('Uploaded')

# 3. List the contents of the directory /activity1/data/

# 4. Move the file /activity1/data/RandomText.txt to /activity1/data/NotSoRandomText.txt

# 5. Append the file RandomText.txt to the end of the file: /activity1/data/RandomText.txt

# 6. List the disk space used by the directory /activity1/data/

# 7. Put the file MoreRandomText.txt into HDFS as the path: /activity1/data/RandomText.txt

# 8. Recursively list the contents of the directory /activity1/

# 9. Remove the directory /activity1/ and all files/directories underneath it
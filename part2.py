#!/usr/bin/python

# @author djeon

from hdfs import InsecureClient
from datetime import datetime
import posixpath as psp
import fs

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

print('Begin')

# 1. Make a directory named: /activity1/
client.makedirs(hdfs_path='/activity1/', permission=None)
client.makedirs(hdfs_path='/activity1/data/', permission=None)

# 2. Put the file RandomText.txt into HDFS as the path: /activity1/data/RandomText.txt
client.upload(hdfs_path='/activity1/data/', local_path='./RandomText.txt')

# 3. List the contents of the directory /activity1/data/
print(client.list('/activity1/data'))

# 4. Move the HDFS file /activity1/data/RandomText.txt to /activity1/data/NotSoRandomText.txt
client.rename('/activity1/data/RandomText.txt', '/activity1/data/NotSoRandomText.txt')

with open('./RandomText.txt', 'r') as f:
	for line in f:
		temp = line

# 5. Append the local file RandomText.txt to the end of the HDFS file: /activity1/data/NotSoRandomText.txt
client.write(hdfs_path='/activity1/data/NotSoRandomText.txt', data=temp, append=True)

# 6. List the disk space used by the directory /activity1/data/
diskSpaceUsed = client.content('/activity1/data/', strict=True)
print(diskSpaceUsed['spaceConsumed'])

# 7. Put the local file MoreRandomText.txt into HDFS as the path: /activity1/data/MoreRandomText.txt
client.upload(hdfs_path='/activity1/data/', local_path='./MoreRandomText.txt')
print(client.list('/activity1/data'))

# 8. Recursively list the contents of the directory /activity1/
fnames = client.list('/activity1')

fpaths = [
	psp.join(dpath, fname)
	for dpath, _, fnames in client.walk('/activity1')
	for fname in fnames
]

print(fpaths)

# 9. Remove the directory /activity1/ and all files/directories underneath it
client.delete(hdfs_path='/activity1', recursive=True)
print(client.list('/'))

print('End')
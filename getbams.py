#
# This script reads Genome Data files in BAM format and converts to SAM format
#
import os
import sys
import swiftclient.client
import pysam

# Replace user and key with username and password
config = {'user':'xxxxx', 
          'key':'xxxxxx',
          'tenant_name':'g2015016',
          'authurl':'http://smog.uppmax.uu.se:5000/v2.0'}

conn = swiftclient.client.Connection(auth_version=2, **config)

(response, bucket_list) = conn.get_account()

objectlist = []
filestoconvert = 1

#Load the Genome container from swift
(response, obj_list) = conn.get_container('GenomeData')
for obj in obj_list: 
    objectname = obj['name']
    if objectname.endswith('.bam'):
	#print objectname
      	objectlist.append(objectname)

count = len(objectlist)
print '### There are ' + str(count) + ' bam files available'
print '### We will convert ' + str(filestoconvert) + ' files'

count = 1

#Convert filestoconvert number of files from BAM to SAM
for obj in objectlist:
	if count > filestoconvert:
		break
	encoded_name = obj.encode('utf-8')    
	(response, obj)=conn.get_object('GenomeData', obj)
	
	sys.stdout = open(encoded_name,'w')
	print obj
        samfilename = encoded_name+'.sam'
	sys.stdout = open(samfilename,'w')
	rows = pysam.view("-S", encoded_name)

	for r in rows:
  		print r
	
	count = count +1
	#Todo Move files to Hadoop HDFS and remove from local file system

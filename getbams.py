#
# This script reads Genome Data files in BAM format and converts to SAM format
#
import os
import sys
import swiftclient.client
import pysam

# Replace user and key with username and password
config = {'user':'xxxxxx', 
          'key':'xxxxxx',
          'tenant_name':'g2015016',
          'authurl':'http://smog.uppmax.uu.se:5000/v2.0'}

conn = swiftclient.client.Connection(auth_version=2, **config)

(response, bucket_list) = conn.get_account()

objectlist = []
filestoconvert = 10

from datetime import datetime
startTime = datetime.now()
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
#sys.exit(0)

count = 1

#Convert filestoconvert number of files from BAM to SAM
for obj in objectlist:
	if obj == 'HG00100.chrom20.ILLUMINA.bwa.GBR.low_coverage.20130415.bam':
		continue  #skip this file gives segmentation fault
	print '### Get file from swift: ' + str(obj)
	if count > filestoconvert:
		break
	encoded_name = obj.encode('utf-8')    
	(response, obj)=conn.get_object('GenomeData', obj)
	print '### Open BAM file for local write: ' + encoded_name
	bamfile = open(encoded_name,'w')
	print '### Writing'
	bamfile.write(obj)
	print '### Close BAM file'
	bamfile.close()
        samfilename = encoded_name+'.sam'
	print '### Open SAM file for local write: ' + samfilename
	samfile = open(samfilename,'w')
	print '### Pysam view get all rows'
	rows = pysam.view("-S", encoded_name)
	print '### Loop rows to write SAM'
	for r in rows:
  		 samfile.write(r)
	print '### Close SAM file'
	samfile.close()
	print '### Remove BAM file'
	os.remove(encoded_name)
	print '### Current file count: ' +str(count)
	count = count +1
	print '### ----------------------------------------'
	#Todo Move files to Hadoop HDFS and remove from local file system
timetaken = datetime.now() - startTime
print '### Time taken: ' + str(timetaken)
print '### Finished'

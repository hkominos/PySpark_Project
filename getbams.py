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
	startTimeSwift = datetime.now()    
	(response, obj)=conn.get_object('GenomeData', obj)
	networktime = datetime.now() - startTimeSwift
	print '--- Transfer time: ' +str(networktime.total_seconds())
	print '### Open BAM file for local write: ' + encoded_name
	bamfile = open(encoded_name,'w')
	print '### Writing'
	startTimeBAMWrite = datetime.now()
	bamfile.write(obj)
	print '### Close BAM file'
	bamfile.close()
	writetimebam = datetime.now() - startTimeBAMWrite
	print '--- Write time BAM: ' + str(writetimebam.total_seconds())
	bamsize = os.path.getsize(encoded_name) >> 20
	print '--- File size BAM (Mb): ' + str(bamsize)
	bampersecond = bamsize / writetimebam.total_seconds()
	swiftpersecond = bamsize / networktime.total_seconds()
	print '--- Swift transfer Mb/sec: ' +str(swiftpersecond)
	print '--- BAM write Mb/sec: ' + str(bampersecond)
        samfilename = encoded_name+'.sam'
	print '### Open SAM file for local write: ' + samfilename
	samfile = open(samfilename,'w')
	print '### Pysam view get all rows'
	starttimepysam = datetime.now()
	rows = pysam.view("-S", encoded_name)
	print '### Loop rows to write SAM'
	for r in rows:
  		 samfile.write(r)
	print '### Close SAM file'
	samfile.close()
	pysamtime = datetime.now() - starttimepysam
	print '--- Convert+write SAM: ' + str(pysamtime.total_seconds())
	samsize = os.path.getsize(samfilename) >> 20
	print '--- File size SAM (Mb): ' + str(samsize)
	sampersecond = samsize / pysamtime.total_seconds()
	print '--- PySAM+Write Mb/sec: ' +str(sampersecond)
	print '### Remove BAM file'
	os.remove(encoded_name)
	print '### Current file count: ' +str(count)
	count = count +1
	print '### ----------------------------------------'
	#Todo Move files to Hadoop HDFS and remove from local file system
timetaken = datetime.now() - startTime
print '=== Total Time taken: ' + str(timetaken)
print '### Finished'

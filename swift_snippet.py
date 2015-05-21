import os
import swiftclient.client
config = {'user':'xxxxxxx', 
          'key':'XXXXXX',
          'tenant_name':'xxxxxxx',
          'authurl':'xxxxxxx'}
conn = swiftclient.client.Connection(auth_version=2, **config)

(response, bucket_list) = conn.get_account()

# List containers
#for bucket in bucket_list:
#    print bucket['name']
#(response, obj_list) = conn.get_container('GenomeData')

#List objects
#for obj in obj_list: 
    #print obj['name']
    
(response, obj)=conn.get_object('GenomeData', 'HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam')
print head_obj(obj)

#print obj

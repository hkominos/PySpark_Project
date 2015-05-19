# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext
"""
#spark-submit --master spark://host-10-0-100-5.openstacklocal:7077 test.py      
                             
#spark set sc
"""
conf = SparkConf()
conf.setAppName("test")
sc = SparkContext(conf = conf)

files=sc.textFile("/BAM/*")
rdd1=files.filter(lambda x:x.split("\t")[8]<1000)
countof=files.count()
print "my count is %i" % (countof)

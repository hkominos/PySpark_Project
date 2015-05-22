# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext
"""
#spark-submit --master spark://host-10-0-100-5.openstacklocal:7077 Spark_filter.py      
                             
#spark set sc
"""
conf = SparkConf()
conf.setAppName("Spark_read")
sc = SparkContext(conf = conf)

files=sc.textFile("/BAM/*")
cleaned=files.filter(lambda line: len(line)>0)
filtered=cleaned.filter(lambda line: abs(int(line.split()[8]))>1000)
oldsize=cleaned.count()
newsize=filtered.count()
filtered.saveAsTextFile("/output")


print "origina size was" % (oldsize)
print "filtered size is  " % (newsize)

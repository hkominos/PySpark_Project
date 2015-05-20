# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext
"""
#spark-submit --master spark://host-10-0-100-5.openstacklocal:7077 test.py      
                             
#spark set sc
"""
conf = SparkConf()
conf.setAppName("Spark_read")
sc = SparkContext(conf = conf)

files=sc.textFile("/BAM/*")
filtered=files.filter(lambda line: abs(int(line.split("\t")[8]))>15)
countof=files.count()
filtered.saveAsTextFile("/output2.txt")


print "my count is %i" % (countof)

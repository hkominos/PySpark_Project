
from pyspark import SparkConf, SparkContext
#spark-submit --master spark://host-10-0-100-5.openstacklocal:6066│ubuntu@host-10-0-100-5:~$ 
# test.py                                           
#spark set sc
conf = SparkConf()
conf.setMaster("local")
conf.setAppName("test")
conf.set("spark.executor.memory", "2g")
sc = SparkContext(conf = conf)

files=sc.textFile("/TWEETS/files/*")
countof=files.count()
print "my count is %i" % (countof)

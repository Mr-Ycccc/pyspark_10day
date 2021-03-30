import  findspark

spark_home = 'C:\\spark-2.4.7-bin-hadoop2.7'
findspark.init(spark_home)

import pyspark
from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName('test').setMaster('local[*]')
sc = SparkContext(conf = conf)

print('spark version:',pyspark.__version__)
rdd = sc.parallelize(['hello','spark'])
print(rdd.reduce(lambda x,y:x+' '+y))

import findspark

spark_home = 'C:\\spark-2.4.7-bin-hadoop2.7'
findspark.init(spark_home)

from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName('test').setMaster('local[*]')
sc = SparkContext(conf = conf)

rdd_line = sc.textFile('C:\code\eat_pyspark_in_10_days-master\data\hello.txt')
print(rdd_line.collect())
rdd_word = rdd_line.flatMap(lambda x:x.split(' '))
print(rdd_word.collect())
rdd_one = rdd_word.map(lambda t:(t,1))
print(rdd_one.collect())
rdd_count = rdd_one.reduceByKey(lambda x,y:x+y)
print(rdd_count.collect())


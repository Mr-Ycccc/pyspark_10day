import findspark
spark_home = 'C:\\spark-2.4.7-bin-hadoop2.7'
findspark.init(spark_home)

from pyspark import SparkConf,SparkContext

conf = SparkConf().setAppName('test').setMaster('local[*]')
sc = SparkContext(conf = conf)

rdd = sc.parallelize(range(1,11),2)
#action操作
print('------------------action-------------')
#collect返回数据到driver，数据量大时会超出内存风险
print(rdd.collect())
#take返回若干个，类似limit？
print(rdd.take(4))
#takesample返回随机若干个
print(rdd.takeSample(False,5,0))
#first返回第一个
print(rdd.first())
#count返回rdd中元素数量
print(rdd.count())
#redunce (1+2)+3+4+5...依次累加
print(rdd.reduce(lambda x,y:x+y))
#foreach对每个元素执行，不生成新rdd，和map功能一样？，action和transformation的区别？
accum = sc.accumulator(0)
rdd.foreach(lambda x:accum.add(x))
print(accum.value)
#countbykey 对rdd按key统计数量
pairrdd = sc.parallelize([(1,1),(1,4),(2,1),(3,1),(1,6),])
print(pairrdd.countByKey())



#transformation操作
print('------------------transformation-------------')
#map对每个元素进行操作
print(rdd.map(lambda x:x+1).collect())
#filter应用过滤条件进行过滤
print(rdd.filter(lambda x:x>5).collect())
#flatMap操作执行将每个元素生成一个Array后压平(先执行map的操作，再将所有对象合并为一个对象，返回值是一个Sequence)
rdd = sc.parallelize(["hello world","hello China"])
print('map:' ,rdd.map(lambda x:x.split(' ')).collect())
print('flapmap:' ,rdd.flatMap(lambda x:x.split(' ')).collect())
#sample对原rdd在每个分区按照比例进行抽样,第一个参数设置是否可以重复抽样
rdd = sc.parallelize(range(10),3)
print('glom:' ,rdd.glom().collect())
print('sample:' , rdd.sample(False,0.7,0).collect())
#
rdd = sc.parallelize([1,1,2,2,3,3,4,5])
print(rdd.distinct().collect())

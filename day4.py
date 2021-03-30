import findspark
spark_home = 'C:\\spark-2.4.7-bin-hadoop2.7'
findspark.init(spark_home)

from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName('day4').setMaster('local[*]')
sc = SparkContext(conf = conf)


data = [1,5,7,10,23,20,6,5,10,7,10]
rdd = sc.parallelize(data)
#求平均数，求data的平均值
# print(rdd.collect())

# rdd_sum = rdd.reduce(lambda x,y:x+y)

# rdd_count = rdd.count()

# avg = rdd_sum / rdd_count

# print(avg)
print('-----------------------------------------------')
#求data中出现次数最多的数
# 每个元素给1计数
rdd_map = rdd.map(lambda x:(x,1))
print(rdd_map.collect())
# 统计每个元素的总数
# rdd_reducebykey = rdd_map.reduceByKey(lambda x,y:x+y)
# result = rdd_map.reduceByKey(lambda x,y:x+y).sortBy(lambda x: x[1],ascending=False).take(1)
# print(result[0][0])
rdd_reducebykey = rdd_map.reduceByKey(lambda x,y:x+y)
print(rdd_reducebykey.collect())
# 取每个元素的计数值
rdd_max = rdd_reducebykey.map(lambda x:x[1])
print(rdd_max.collect())
# 比对每个计数值的大小
rdd_max_value = rdd_max.reduce(lambda x,y:x if x > y else y)
print(rdd_max_value)
# 根据最大的计数值找出元素的tuple
rdd_max_key = rdd_reducebykey.filter(lambda x:x[1]==rdd_max_value)
print(rdd_max_key.collect())
# 从tuple取元素
rdd_max_num = rdd_max_key.map(lambda x:x[0])
print(rdd_max_num.collect())


print('-----------------------------------------------')
# 有一批学生信息表格，包括name,age,score, 找出score排名前3的学生, score相同可以任取
students = [("LiLei",18,87),("HanMeiMei",16,77),("DaChui",16,66),("Jim",18,77),("RuHua",18,50)]
rdd_students = sc.parallelize(students)
rdd_sort = rdd_students.sortBy(lambda x:x[2],ascending=False).take(3)
print(rdd_sort)


print('-----------------------------------------------')
#任务：按从小到大排序并返回序号, 大小相同的序号可以不同
data = [1,7,8,5,3,18,34,9,0,12,8]
rdd_data = sc.parallelize(data)
rdd_data_sort = rdd_data.sortBy(lambda x:x)
print(rdd_data_sort.collect())
rdd_data_index = rdd_data_sort.zipWithIndex()
print(rdd_data_index.collect())


print('-----------------------------------------------')
#任务：已知班级信息表和成绩表，找出班级平均分在75分以上的班级
#班级信息表包括class,name,成绩表包括name,score

classes = [("class1","LiLei"), ("class1","HanMeiMei"),("class2","DaChui"),("class2","RuHua")]
scores = [("LiLei",76),("HanMeiMei",80),("DaChui",70),("RuHua",60)]

rdd_classes = sc.parallelize(classes).map(lambda x:(x[1],x[0]))
rdd_scores = sc.parallelize(scores)
rdd_join = rdd_classes.join(rdd_scores)
print(rdd_join.collect())

def average(iterator):
    data = list(iterator)
    s = 0.0
    for x in data:
        s = s + x
    return s/len(data)

rdd_groupbykey = rdd_join.groupByKey()
print(rdd_groupbykey.collect())
rdd_classes_map = rdd_groupbykey.map(lambda t:(t[0],average(t[1])))
print(rdd_classes_map.collect())
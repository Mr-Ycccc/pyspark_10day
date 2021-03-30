import findspark
spark_home = 'C:\\spark-2.4.7-bin-hadoop2.7'
findspark.init(spark_home)


import pyspark
from pyspark.sql import SparkSession

import pandas as pd

spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()

sc = spark.sparkContext


# 通过toDF方法转换成DataFrame,可以将RDD用toDF方法转换成DataFrame
rdd = sc.parallelize([("LiLei",15,88),("HanMeiMei",16,90),("DaChui",17,60)])
print(rdd.collect())
df = rdd.toDF(['name','age','score'])
print(type(df))
df.show()
df.printSchema()

# 通过createDataFrame方法将Pandas.DataFrame转换成pyspark中的DataFrame
pdf = pd.DataFrame([("LiLei",18),("HanMeiMei",17)],columns = ["name","age"])
print(type(pdf))
df= spark.createDataFrame(pdf)
df.show()

# 对列表直接转换
values = [("LiLei",18),("HanMeiMei",17)]
df = spark.createDataFrame(values,['name','age'])
df.show()

from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime

schema = StructType([StructField("name", StringType(), nullable = False),
                     StructField("score", IntegerType(), nullable = True),
                     StructField("birthday", DateType(), nullable = True)])

rdd = sc.parallelize([Row("LiLei",87,datetime(2010,1,5)),
                      Row("HanMeiMei",90,datetime(2009,3,1)),
                      Row("DaChui",None,datetime(2008,7,2))])

dfstudent = spark.createDataFrame(rdd,schema)
dfstudent.show()
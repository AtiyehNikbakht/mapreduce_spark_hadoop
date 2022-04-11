import pyspark
from pyspark.sql import SparkSession
import time

start = time.time()
spark = SparkSession.builder.appName('SparkPractice').getOrCreate()
sc = spark.sparkContext

#Converting Text to RDD and doing map reduce function
textRdd = sc.textFile("/user/ebrahimi/hw3-data/file1G.txt")
textRddM = textRdd.flatMap(lambda x: x.split(' '))
textRddM = textRddM.map(lambda x: (x,1))
textRddM = textRddM.reduceByKey(lambda x,y: x+y)
textRddM = textRddM.collect()
end = time.time()
final = end - start

print("Time is:", final)
spark.stop()


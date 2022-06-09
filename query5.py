import findspark
findspark.init()
findspark.find()


import pyspark # only run after findspark.init()
import pyspark.sql.functions as func

import time

start=time.time()

# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit



# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 

#dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
#rdd=spark.sparkContext.parallelize(dataList)

data = spark.read.csv(
    'final_reviews_final.csv',
    sep = ',',
    header = True
    )

#data.show(5)
result=data.groupBy('region', 'year', 'month').count().alias('Number_of_reviews').where(col('month')==12)
result=result.sort('region','year','month')
#result.write.csv('query5_output_sorted.csv')
#result.toPandas().to_csv('query5_pd.csv')

result.show(5)
end=time.time()

print ("Execution time:", end-start)

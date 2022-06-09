import findspark
findspark.init()
findspark.find()


import pyspark # only run after findspark.init()
import pyspark.sql.functions as func



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

reviews = spark.read.csv(
    'final_reviews_final.csv',
    sep = ',',
    header = True
    )

calendars = spark.read.csv(
    'final_calendars_final.csv',
    sep = ',',
    header = True
    )

listings = spark.read.csv(
    'final_listings.csv',
    sep = ',',
    header = True
    )


result_1=reviews.groupBy('reviewer_id', 'listing_id').count().where(col('count')>3)
result_2=result_1.alias("a").join(reviews.alias("r"), (result_1.reviewer_id==reviews.reviewer_id) & (result_1.listing_id==reviews.listing_id)).select('a.reviewer_id','a.listing_id','r.reviewer_name', 'r.year','r.month')

result_2=result_2.na.drop()

result_3=result_2.alias("a").join(calendars, (calendars.listing_id==result_2.listing_id) & (calendars.month==result_2.month)).where(col('available')=='t').select('a.listing_id','a.reviewer_name')

final_result=result_3.alias("r").join(listings.alias("l"), result_3.listing_id==listings.id).select('l.name','l.listing_url', 'l.description', 'l.host_id', 'l.minimum_nights','l.maximum_nights', 'l.has_availability', 'l.availability_30', 'l.availability_60', 'l.availability_90', 'l.availability_365','r.reviewer_name')


final_result2=listings.alias("l").join(final_result.alias("r"), listings.host_id==final_result.host_id).select('l.name','l.listing_url', 'l.description', 'l.host_name', 'l.minimum_nights','l.maximum_nights', 'l.has_availability', 'l.availability_30', 'l.availability_60', 'l.availability_90', 'l.availability_365','r.reviewer_name')


final_result2.show(5)

final_result2=final_result2.distinct()
#result.write.csv('query5_output_sorted.csv')
#final_result2.toPandas().to_csv('query6_pd_2.csv')

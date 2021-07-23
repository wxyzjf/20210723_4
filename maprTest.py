from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("spark connection maprdb test").setMaster('local[5]')
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .getOrCreate()

#spark.loadFromMapRDB("maprfs:///HDS_VOL_TMP/test_mall_ba/newTable").show()


df = sc.parallelize([ { "_id": "454065216321941", "eci":"123456", "eciout": "123457", "time": "123456780", "timeout":"123456789"}]).toDF().orderBy("_id")

spark.insertToMapRDB(df,"maprfs:///HDS_VOL_TMP/test_mall_ba/newTable",bulk_insert=True)

spark.loadFromMapRDB("maprfs:///HDS_VOL_TMP/test_mall_ba/newTable").show()

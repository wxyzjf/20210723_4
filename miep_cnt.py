from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
spark = SparkSession.builder.appName("sp_miep").getOrCreate()
sc = spark.sparkContext

df = spark.read.parquet("maprfs:///HDS_VOL_TMP/test_miep/*parquet")
df.take(5)

df.printSchema()

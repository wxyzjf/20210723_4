from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
##spark = SparkSession.builder.appName("Test App").getOrCreate()

#textFile = spark.read.text("maprfs:///HDS_VOL_HIVE/FWCDR/start_date=20200220/fwcdr_ldr_20200220_20200222093947_p1_000.gz")
textFile = spark.read.text("maprfs:///HDS_VOL_HIVE/NOTICE.txt")

cc=textFile.count()
print ("Line count c:%i",cc)

spark.stop()

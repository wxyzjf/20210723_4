from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("Test App").setMaster('local[5]')
sc = SparkContext(conf=conf)

#textFile = spark.read.text("maprfs:///HDS_VOL_HIVE/FWCDR/start_date=20200220/fwcdr_ldr_20200220_20200222093947_p1_000.gz")
#textFile = spark.read.text("maprfs:///HDS_VOL_HIVE/NOTICE.txt")

def func_map(s):
    if int(s)%2==0:
        return (s,1)

def func_map2(k):
    return k + 1000

def func_part(iterator):
	print "-----" + iterator 

cnt = sc.textFile("file:///home/mapr/sp/test.txt",2).count()
#pairs = lines.map(func_map)
#pairs.collect()
#pairs = lines.mapPartitions(func_part)

#cc = pairs.reduceByKey(lambda a,b:a+b).sortByKey()

#cc = pairs.sortByKey()


#list=cc.collect()
#for f in list:
#	print f
#print pairs.getNumPartitions()
#print ("Partitions structure :{}".format(pairs.glom().collect()))

print ("Partitions structure :{}%d" , cnt)

from pyspark.sql import Row

myRow = Row("Hello",None,1,False)
print(myRow.printSchema())

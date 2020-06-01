from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession


#fetching csv files from HDP
csv_data = sc.textFile("hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/csv_data/yellow_tripdata/")
csv_data  = csv_data.map(lambda p: p.split(","))
header = csv_data.first()
csv_data = csv_data.filter(lambda p:p != header)

#making a dataframe
#common error when casting to tables (contains invalid character(s) among " ,;{}()\n\t=")
header_row = Row(*header)
df = csv_data.map(lambda p: header_row(*p[:len(header)])).toDF()
df.createGlobalTempView("yellow")
df.printSchema()


#avg(CAST(fare_amount AS DOUBLE)) need to cast other integers and doubles
spark.sql("SELECT vendorid, max(trip_distance), min(trip_distance) FROM global_temp.yellow group by vendorid").show()
spark.sql("SELECT vendorid, sum(trip_distance) FROM global_temp.yellow group by vendorid").show()
spark.sql("SELECT PULocationID, count(*) FROM global_temp.yellow group by PULocationID").show()
spark.sql("SELECT vendorid, PULocationID, DOLocationID FROM global_temp.yellow where PULocationID = DOLocationID").show()
spark.sql("SELECT vendorid, avg(fare_amount), max(fare_amount), min(fare_amount) FROM global_temp.yellow group by vendorid").show()

'''
#to add the dataframes to hive tables
from pyspark.sql import HiveContext
hc = HiveContext(sc)
df_csv.write.format("orc").saveAsTable("employees")
'''
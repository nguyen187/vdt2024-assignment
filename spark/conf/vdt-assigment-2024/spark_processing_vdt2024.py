

from pyspark.sql import SparkSession

from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create SparkSession
spark = SparkSession.builder \
    .appName("VDT DE 2024") \
    .getOrCreate()
# schema for input data
studentActivity = StructType() \
                    	.add("student_code", "integer")\
                    	.add("activity", "string")\
                    	.add("numberOfFile", "integer")\
                    	.add("timestamp", "string")

# path file Parquet in HDFS
parquet_file_path = "hdfs://192.168.117.128:9010/raw_zone/fact/activity/"

# read parquet file in HDFS
# print("Starting reading log action file from HDFS...")
df = spark.read.schema(schema=studentActivity).parquet(parquet_file_path)

#convert timstamp to format 20240615
df= df.withColumn("timestamp", date_format(to_date(df["timestamp"], "M/d/yyyy"), "yyyyMMdd"))

# groupby and count
df_procesed = df.groupBy("student_code", "activity", "timestamp").agg(sum("numberOfFile").alias("total_files"))

# read file information student de vdt2024 from hdfs
schema_ds_de = StructType() \
                    	.add("code", "integer")\
                    	.add("name", "string")


ds_de = spark.read.schema(schema=schema_ds_de).csv("hdfs://192.168.117.128:9010/raw_zone/fact/ds_de_vdt/danh_sach_sv_de.csv")

# join to take name student
ans = ds_de.join(df_procesed,ds_de.code ==  df_procesed.student_code,"inner").select('timestamp','code','name','activity','total_files')

# extract distinct name student 
names = [row.name for row in ans.select("name").distinct().collect()]

# filter with each student and save to file csv with namestudent.csv at current directory
for name in names:
    df_filtered = ans.filter(ans.name == name)
    # print(f"Writing ./output/{name}.csv")
    # df_filtered.toPandas().to_csv("./output/"+name+".csv",header=False,index=False)
    df_filtered.write.mode("overwrite").csv("./output/"+name+".csv")

print("Done Assigment VDT 2024 <3")

# -------------------------------------Finish Assigment VDT 2024--------------------------------------
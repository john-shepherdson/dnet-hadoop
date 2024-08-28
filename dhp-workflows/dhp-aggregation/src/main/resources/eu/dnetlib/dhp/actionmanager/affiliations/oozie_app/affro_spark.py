#!/usr/bin/python3

from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import sys

from affro_cluster import *

# Initialize SparkSession
spark = SparkSession.builder.appName("CustomFunctionExample").getOrCreate()

output_folder = sys.argv[1]
print("Writing to folder: ", output_folder)

# Register the function as a UDF
affro_udf = udf(affro, StringType())

# Input list of strings
input_data = ["university of athens", "university of vienna", "UCLA"]

# # Convert the list to a Spark DataFrame
df = spark.createDataFrame(input_data, "string").toDF("raw_affiliation_string")

# # Apply your custom UDF to the DataFrame
df_with_custom_value = df.withColumn("affro_value", affro_udf(df["raw_affiliation_string"]))


# df_with_custom_value.show(truncate=False)
df_with_custom_value.write.mode("overwrite").option("delimiter", "\t").csv(output_folder)


# Stop the SparkSession
spark.stop()

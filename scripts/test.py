import os
from pyspark.sql import SparkSession

# Fix temporaire pour Ivy
os.environ['IVY_HOME'] = '/tmp/.ivy2'

# Lancer Spark
spark = SparkSession.builder \
    .appName("TestSparkSession") \
    .config("spark.jars.ivy", "/tmp/.ivy2") \
    .getOrCreate()

df = spark.createDataFrame([(1, "test")], ["id", "value"])
df.show()

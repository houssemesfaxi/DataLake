from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExportPostgresToMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://1abaed8e5fc0:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/external_jars/postgresql-42.7.3.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://66da2849d96d:5432/airflowdb"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Exemple : lire la table 
tables = ["facture", "fournisseur", "achat"]
for table in tables:
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
    df.write.mode("overwrite").option("header", True).option("delimiter", ",") .csv(f"s3a://testdata/{table}_export")


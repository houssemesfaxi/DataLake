from pyspark.sql import SparkSession

# 1. Initialiser SparkSession
spark = SparkSession.builder \
    .appName("ImportFacturesToPostgres") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://1abaed8e5fc0:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/external_jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# 2. Lire le fichier CSV depuis MinIO
df = spark.read.option("header", True).option("delimiter", ";") \
    .csv("s3a://testdata/factures_2025.csv")

# 3. Paramètres PostgreSQL
postgres_url = "jdbc:postgresql://66da2849d96d:5432/airflowdb"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# 4. Insérer dans la table "facture"
df.write.jdbc(
    url=postgres_url,
    table="facture_temp",
    mode="overwrite",  # utilise "append" si tu ne veux pas écraser
    properties=properties
)

print("✅ Données importées avec succès dans la table PostgreSQL.")

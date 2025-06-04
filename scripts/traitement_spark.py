from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, trim, lower, when # type: ignore

# === 1. Initialiser Spark avec accès MinIO ===
spark = SparkSession.builder \
    .appName("CleanCSStudentsData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/external_jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# === 2. Lire les données depuis MinIO ===
df = spark.read.csv("s3a://testdata/cs_students.csv", header=True, inferSchema=True)

# === 3. Standardiser les noms de colonnes ===
for old_col in df.columns:
    df = df.withColumnRenamed(old_col, old_col.strip().replace(" ", "_"))

# === 4. Nettoyer les colonnes de compétences ===
def encode_level(colname):
    return (
        when(lower(trim(col(colname))) == "weak", 1)
        .when(lower(trim(col(colname))) == "average", 2)
        .when(lower(trim(col(colname))) == "strong", 3)
        .otherwise(None)
    )

df_clean = (
    df.withColumn("Python_Level", encode_level("Python"))
      .withColumn("SQL_Level", encode_level("SQL"))
      .withColumn("Java_Level", encode_level("Java"))
      .drop("Python", "SQL", "Java")
      .dropDuplicates()
      .na.drop()
)

# === 5. Aperçu et schéma ===
df_clean.show(5)
df_clean.printSchema()

# === 6. Sauvegarder dans PostgreSQL ===
(
    df_clean.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres_db1:5432/airflowdb")
    .option("dbtable", "students_clean")  # table dans airflowdb
    .option("user", "airflow")
    .option("password", "airflow")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()

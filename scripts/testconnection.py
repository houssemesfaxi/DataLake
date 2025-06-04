from pyspark.sql import SparkSession

# Créer la session Spark avec configuration PostgreSQL
spark = SparkSession.builder \
    .appName("TestPostgresConnection") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://1abaed8e5fc0:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.jars", "/external_jars/postgresql-42.7.3.jar") \
    .getOrCreate()

try:
    df_test = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres_db1:5432/airflowdb") \
        .option("dbtable", "information_schema.tables") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .load()

    print("✅ Connexion PostgreSQL réussie")
    df_test.show(5)
except Exception as e:
    print("❌ Erreur de connexion à PostgreSQL :")
    print(e)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import re
from collections import namedtuple

# 1. Initialiser SparkSession
spark = SparkSession.builder \
    .appName("TextMiningFactures") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://1abaed8e5fc0:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. Charger le fichier texte OCR
rdd = spark.sparkContext.textFile("s3a://testdata/factures_ocr_result.txt")

# 3. Séparer les factures (en blocs) par "====="
raw_text = rdd.collect()
factures = "\n".join(raw_text).split("=====")

# 4. Définir le schéma et la structure
Facture = namedtuple("Facture", [
    "num_facture", "date", "code_client", "nom_client", "adresse", "mat_fiscal",
    "ref", "designation", "qte", "prix_unit", "montant", "tva"
])

# 5. Nouvelle regex pour les dates au format : "Date 18. avril 2023"
date_pattern = re.compile(r"Date\s+(\d{1,2}[\.]?\s*\w+\s*\d{4})", re.IGNORECASE)

# 6. Fonction d'extraction par bloc
data = []
for bloc in factures:
    num_facture = date = code_client = nom_client = adresse = mat_fiscal = ""
    lines = bloc.strip().split('\n')

    for line in lines:
        if "N° VF" in line:
            m = re.search(r"N° VF\s+(\d+/\d+)", line)
            if m: num_facture = m.group(1)
        if "Date" in line:
            m = date_pattern.search(line)
            if m: date = m.group(1)
        if "Code Client" in line:
            m = re.search(r"Code Client\s+([A-Z0-9]+)", line)
            if m: code_client = m.group(1)
        if "Nom Client" in line:
            m = re.search(r"Nom Client[:\s]+(.+)", line)
            if m: nom_client = m.group(1).strip()
        if "Adresse" in line:
            m = re.search(r"Adresse[:\s]+(.+)", line)
            if m: adresse = m.group(1).strip()
        if "Mat. Fiscal" in line:
            m = re.search(r"Mat\.? Fiscal[:\s]+([^\s]+)", line)
            if m: mat_fiscal = m.group(1)

        # Extraction d'article
        article = re.search(r"([A-Z0-9\-]+)\s+(.+?)\s+(\d+)\s+([\d\.,]+)\s+([\d\.,]+)\s+(\d+ ?%)", line)
        if article:
            ref = article.group(1).strip()
            designation = article.group(2).strip()
            qte = int(article.group(3))
            prix_unit = float(article.group(4).replace(',', '.'))
            montant = float(article.group(5).replace(',', '.'))
            tva = article.group(6).strip()

            data.append(Facture(
                num_facture, date, code_client, nom_client, adresse, mat_fiscal,
                ref, designation, qte, prix_unit, montant, tva
            ))

# 7. Créer un DataFrame Spark
schema = StructType([
    StructField("num_facture", StringType(), True),
    StructField("date", StringType(), True),
    StructField("code_client", StringType(), True),
    StructField("nom_client", StringType(), True),
    StructField("adresse", StringType(), True),
    StructField("mat_fiscal", StringType(), True),
    StructField("ref", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("qte", IntegerType(), True),
    StructField("prix_unit", FloatType(), True),
    StructField("montant", FloatType(), True),
    StructField("tva", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)
df = df.coalesce(1)
# 8. Export vers MinIO (ou local avec sep=point-virgule si besoin)
df.write.option("header", True).option("delimiter", ";").mode("overwrite") \
    .csv("s3a://testdata/factures_structurées_final")

print("\u2705 Extraction et export terminé avec colonne Date corrigée.")

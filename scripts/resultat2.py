from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import re
from collections import namedtuple

# Initialisation Spark
spark = SparkSession.builder \
    .appName("ExtractionFactures") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://1abaed8e5fc0:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Lecture fichier OCR
rdd = spark.sparkContext.textFile("s3a://testdata/factures_ocr_result2.txt")
text = "\n".join(rdd.collect())
factures = text.split("=====")

# Définir la structure
Facture = namedtuple("Facture", [
    "num_facture", "fournisseur", "date", "adresse_fournisseur", "telephone", "fax",
    "email", "produit", "montant", "tva", "net_a_payer"
])

data = []

# Expressions régulières pour extraire les champs
for bloc in factures:
    num_facture = fournisseur = date = adresse_fournisseur = telephone = fax = email = produit = montant = tva = net_a_payer = ""

    lines = bloc.strip().split("\n")

    for line in lines:
        if not fournisseur:
            m = re.search(r"Fournisseur[:\s]+(.+)", line)
            if m: fournisseur = m.group(1).strip()

        if not num_facture:
            m = re.search(r"(N°|Facture\s)?[\s]*([A-Z]?\d+[-/]?\d*)", line)
            if m: num_facture = m.group(2).strip()

        if not date:
            m = re.search(r"Date\s*[:\-]?\s*(\d{1,2}\s\w+\s\d{4}|\d{2}[-/\.]\d{2}[-/\.]\d{4})", line, re.IGNORECASE)
            if m: date = m.group(1)

        if not adresse_fournisseur:
            m = re.search(r"Adresse Fournisseur[:\s]+(.+)", line, re.IGNORECASE)
            if m: adresse_fournisseur = m.group(1).strip()

        if not telephone:
            m = re.search(r"T[ée]l[éphone.:\- ]+([\+\d\s\(\)/]+)", line)
            if m: telephone = m.group(1).strip()

        if not fax:
            m = re.search(r"Fax[:\- ]+([\d\s\(\)]+)", line)
            if m: fax = m.group(1).strip()

        if not email:
            m = re.search(r"[Ee]-?mail[:\- ]+([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)", line)
            if m: email = m.group(1).strip()

        if not produit:
            m = re.search(r"(Honoraires|Débours|Produit|SAC A DOS|Assistance Comptable).+", line)
            if m: produit = m.group(0).strip()

        if not montant:
            m = re.search(r"(Soit HT|Montant|Total TTC|Total)\s*:?\s*([\d\.,]+)", line)
            if m: montant = m.group(2).replace(',', '.')

        if not tva:
            m = re.search(r"TVA\s+[\w%]*\s*([\d\.,]+)", line)
            if m: tva = m.group(1).replace(',', '.')

        if not net_a_payer:
            m = re.search(r"Net [àa] Payer\s*:?\s*([\d\.,]+)", line)
            if not m:
                m = re.search(r"TOTAL [ÀA] PAYER\s*:?\s*([\d\.,]+)", line)
            if m: net_a_payer = m.group(1).replace(',', '.')

    data.append(Facture(
        num_facture, fournisseur, date, adresse_fournisseur, telephone, fax,
        email, produit, montant, tva, net_a_payer
    ))

# Structuration du DataFrame
schema = StructType([
    StructField("num_facture", StringType(), True),
    StructField("fournisseur", StringType(), True),
    StructField("date", StringType(), True),
    StructField("adresse_fournisseur", StringType(), True),
    StructField("telephone", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("email", StringType(), True),
    StructField("produit", StringType(), True),
    StructField("montant", StringType(), True),
    StructField("tva", StringType(), True),
    StructField("net_a_payer", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)

# Export vers MinIO (fichier unique, CSV avec délimiteur ;)
df.coalesce(1).write.option("header", True).option("delimiter", ";").mode("overwrite") \
    .csv("s3a://testdata/factures_structurées_final2")

print("✅ Export terminé vers MinIO avec toutes les données extraites.")


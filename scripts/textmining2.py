from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import re
from collections import namedtuple
import boto3

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
rdd = spark.sparkContext.textFile("s3a://testdata/factures_ocr_result1.txt")

# 3. Séparer les factures (en blocs) par "====="
raw_text = rdd.collect()
factures = "\n".join(raw_text).split("=====")

# 4. Définir le schéma et la structure
Facture = namedtuple("Facture", [
    "num_facture", "fournisseur", "date", "code_client", "tel_fournisseur", "adresse_F", "fax_fournisseur",
    "ref", "designation", "qte", "prix_unit", "montant", "tva"
])

# 5. Nouvelle regex pour les dates au format : "Date 18. avril 2023"
date_pattern = re.compile(r"Date\s+(\d{1,2}[\.]?\s*\w+\s*\d{4})", re.IGNORECASE)

# 6. Fonction d'extraction par bloc
data = []
for bloc in factures:
    num_facture  = fournisseur = date = code_client = tel_fournisseur = adresse_F = fax_fournisseur = ""
    lines = bloc.strip().split('\n')

    for line in lines:
        if not fournisseur:
            m = re.search(r"Fournisseur[:\s]+(.+)", line)
            if m: fournisseur = m.group(1).strip()
        if "N° VF" in line:
            m = re.search(r"N° VF\s*([A-Z]*\d+/\d+|[A-Z]+\d+|\d+/\d+)", line)
            if m: num_facture = m.group(1)
        if "Date" in line:
            m = date_pattern.search(line)
            if m: date = m.group(1)
        if "Code Client" in line:
            m = re.search(r"Code Client\s+([A-Z0-9]+)", line)
            if m: code_client = m.group(1)
        if "N° téléphone" in line:
            m_tel = re.search(r"N°\s*téléphone\s*[:\-]?\s*(\d{2}[\s\.]?\d{3}[\s\.]?\d{3,4})", line, re.IGNORECASE)
            if m_tel:
                tel_fournisseur = m_tel.group(1)
        if "Adresse" in line:
            m = re.search(r"Adresse\s*[:\-]?\s*(.+)", line)
            if m: adresse_F = m.group(1).strip()
        if "Fax" in line:
            m_fax = re.search(r"Fax\s*[:\-]?\s*(\d{2}[\s\.]?\d{3}[\s\.]?\d{3,4})", line, re.IGNORECASE)
            if m_fax:
                fax_fournisseur = m_fax.group(1)

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
                num_facture, fournisseur, date, code_client, tel_fournisseur, adresse_F, fax_fournisseur,
                ref, designation, qte, prix_unit, montant, tva
            ))

# 7. Créer un DataFrame Spark
schema = StructType([
    StructField("num_facture", StringType(), True),
    StructField("fournisseur", StringType(), True),
    StructField("date", StringType(), True),
    StructField("code_client", StringType(), True),
    StructField("tel_fournisseur", StringType(), True),
    StructField("adresse_F", StringType(), True),
    StructField("fax_fournisseur", StringType(), True),
    StructField("ref", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("qte", IntegerType(), True),
    StructField("prix_unit", FloatType(), True),
    StructField("montant", FloatType(), True),
    StructField("tva", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)
df = df.coalesce(1)
# 8. Export vers MinIO
export_path = "s3a://testdata/factures_structurées_final"
df.write.option("header", True).option("delimiter", ";").mode("overwrite").csv(export_path)

# 9. Renommer le fichier généré dans MinIO
s3 = boto3.client('s3',
    endpoint_url="http://1abaed8e5fc0:9000",
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin')

response = s3.list_objects_v2(Bucket="testdata", Prefix="factures_structurées_final/")
for obj in response.get("Contents", []):
    key = obj["Key"]
    if key.endswith(".csv"):
        s3.copy_object(
            Bucket="testdata",
            CopySource=f"testdata/{key}",
            Key="factures_2025.csv"
        )
        s3.delete_object(Bucket="testdata", Key=key)
        print(f"✅ Fichier renommé : {key} → factures_2025.csv")
        break
else:
    print("❌ Aucun fichier CSV trouvé à renommer.")

print("✅ Extraction, export et renommage terminé.")


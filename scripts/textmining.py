from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import re

# 1. Initialiser Spark
spark = SparkSession.builder \
    .appName("TextMiningFactures") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://1abaed8e5fc0:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. Charger le fichier texte OCR depuis MinIO
rdd = spark.sparkContext.textFile("s3a://testdata/factures_ocr_result.txt")


# 3. Fonction d'extraction des données par bloc
def parse_facture_block(block):
    lines = block.strip().split('\n')
    results = []
    num_facture = date = code_client = nom_client = adresse = mat_fisc = ""

    for line in lines:
        if "N° VF" in line:
            m = re.search(r"N° VF\s*[\w ]*(\d+/\d+)", line)
            if m: num_facture = m.group(1)
        if "Date" in line:
            m = re.search(r"Date\s+(\d{1,2}[\. ]\w+\s?\d{4})", line)
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
            if m: mat_fisc = m.group(1)

        # Lignes articles
        art = re.search(r"([A-Z0-9\-]+)\s+(.+?)\s+(\d+)\s+([\d\.,]+)\s+([\d\.,]+)\s+(\d+ ?%)", line)
        if art:
            ref = art.group(1).strip()
            designation = art.group(2).strip()
            qte = int(art.group(3))
            prix_unit = float(art.group(4).replace(',', '.'))
            montant = float(art.group(5).replace(',', '.'))
            tva = art.group(6).strip()

            results.append((num_facture, date, code_client, nom_client, adresse, mat_fisc,
                            ref, designation, qte, prix_unit, montant, tva))
    return results

# 4. Séparer les factures (en blocs) par "====="
raw_text = rdd.collect()
blocks = "\n".join(raw_text).split("=====")

# 5. Appliquer l'extraction sur chaque bloc
parsed = []
for bloc in blocks:
    parsed.extend(parse_facture_block(bloc))

# 6. Créer un DataFrame Spark structuré
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
    StructField("tva", StringType(), True),
])

df = spark.createDataFrame(parsed, schema=schema)

# 7. Export en CSV
df.coalesce(1).write.option("header", True).mode("overwrite").csv("s3a://testdata/factures_final.csv")

print("✅ Extraction et export terminé.")

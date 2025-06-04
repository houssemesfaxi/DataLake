from pyspark.sql import SparkSession
from pdf2image import convert_from_bytes
import pytesseract
import boto3
import re
import os
import shutil
from datetime import datetime

# === 1. Init Spark ===
spark = SparkSession.builder \
    .appName("ScraperCVMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://1abaed8e5fc0:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/external_jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# === 2. Connexion MinIO ===
minio_client = boto3.client(
    's3',
    endpoint_url='http://1abaed8e5fc0:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

bucket = 'testdata'
prefix = 'facture/'

# === 3. Fichiers PDF dans 'facture/' ===
response = minio_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
pdf_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.pdf')]

# === 4. Listes de détection ===
COMPETENCES_CLES = [
    "Python", "Java", "C", "C++", "R", "SQL", "NoSQL", "MongoDB", "MySQL",
    "Spark", "Hadoop", "Hive", "Scikit-learn", "Pandas", "Power BI", "React",
    "Node.js", "Symfony", "Flutter", "Android", "Azure", "Linux"
]

MOTS_FORMATION = ["ESPRIT", "Manouba", "Université", "Licence", "Diploma",
                  "Engineering", "Applied", "Cycle", "Baccalaureate"]

# === 5. Traitement OCR ===
data = []
for key in pdf_keys:
    print(f"📄 Traitement de : {key}")
    try:
        content = minio_client.get_object(Bucket=bucket, Key=key)['Body'].read()
        images = convert_from_bytes(content)
        full_text = " ".join([pytesseract.image_to_string(img) for img in images])

        # Extraire info
        email = re.findall(r'[\w\.-]+@[\w\.-]+', full_text)
        phone = re.findall(r'\+216[\s\-]?\d{2}[\s\-]?\d{3}[\s\-]?\d{3}', full_text)
        linkedin = re.findall(r'https?://(www\.)?linkedin\.com/in/[^\s,]+', full_text)
        github = re.findall(r'https?://(www\.)?github\.com/[^\s,]+', full_text)

        # Compétences
        competences_extraites = [tech for tech in COMPETENCES_CLES if tech.lower() in full_text.lower()]

        # Formations
        formation_extraites = [line.strip() for line in full_text.split('\n')
                               if any(mot.lower() in line.lower() for mot in MOTS_FORMATION)]

        data.append([
            key,
            email[0] if email else "",
            phone[0] if phone else "",
            linkedin[0] if linkedin else "",
            github[0] if github else "",
            ", ".join(competences_extraites),
            " | ".join(formation_extraites[:3]),
            full_text.replace('\n', ' ')
        ])
    except Exception as e:
        print(f"❌ Erreur avec {key} : {e}")
        data.append([key, "", "", "", "", "", "", f"Erreur: {str(e)}"])

# === 6. Créer DataFrame ===
columns = ["Fichier", "Email", "Téléphone", "LinkedIn", "GitHub", "Compétences", "Formations", "Texte_Complet"]

# Assure que data est bien une liste de listes (pas de None ni de dicts)
data_clean = [row for row in data if isinstance(row, list) and len(row) == len(columns)]
df = spark.createDataFrame(data_clean, columns)

# === 7. Sauvegarde CSV structuré unique ===
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = f"/tmp/cv_extraits_temp_{timestamp}"
final_csv = f"/tmp/cv_extraits_{timestamp}.csv"


os.makedirs(output_dir, exist_ok=True)
df.write.format("csv").option("header", True).mode("overwrite").save(output_dir)

# === 8. Fusion part-*.csv -> fichier unique ===
with open(final_csv, 'w', encoding='utf-8') as outfile:
    for i, filename in enumerate(sorted(os.listdir(output_dir))):
        if filename.startswith("part-") and filename.endswith(".csv"):
            with open(os.path.join(output_dir, filename), 'r', encoding='utf-8') as infile:
                if i > 0:
                    infile.readline()  # skip header
                shutil.copyfileobj(infile, outfile)

print(f"✅ Fichier final généré : {final_csv}")
print(f"📊 Total de CV traités : {len(data_clean)}")

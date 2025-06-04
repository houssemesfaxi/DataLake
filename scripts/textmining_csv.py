import boto3
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import io
import os
import csv

# === Configuration Tesseract ===
pytesseract.pytesseract.tesseract_cmd = "/usr/bin/tesseract"  # Chemin dans ton conteneur

# === Connexion à MinIO ===
minio_client = boto3.client(
    's3',
    endpoint_url='http://1abaed8e5fc0:9000',  # adapte selon ton hostname réseau
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

bucket_name = 'testdata'
prefix = 'facture/'  # chemin dans le bucket si tu as des sous-dossiers

# === Fonction pour extraire texte OCR d’un PDF scanné ===
def extract_text_from_pdf(pdf_bytes):
    text = ""
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            pix = page.get_pixmap(dpi=300)
            img_data = pix.tobytes("png")
            image = Image.open(io.BytesIO(img_data))
            text += pytesseract.image_to_string(image)
    return text

# === Préparation du fichier CSV en local ===
output_csv_path = "/scripts/ocr_factures.csv"  # adapte le chemin si nécessaire
os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

with open(output_csv_path, mode='w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["filename", "extracted_text"])  # entêtes

    # === Parcours des objets PDF dans MinIO ===
    response = minio_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".pdf"):
            continue

        print(f"📥 Traitement du fichier : {key}")
        try:
            pdf_obj = minio_client.get_object(Bucket=bucket_name, Key=key)
            pdf_bytes = pdf_obj["Body"].read()
            extracted_text = extract_text_from_pdf(pdf_bytes)
            writer.writerow([key, extracted_text.replace('\n', ' ')])
            print(f" Extraction terminée pour : {key}")
        except Exception as e:
            print(f"Erreur sur {key} : {e}")

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.lower().endswith(".pdf"):
            print(f"Processing: {key}")
            pdf_obj = minio_client.get_object(Bucket=bucket_name, Key=key)
            pdf_bytes = pdf_obj['Body'].read()
            extracted_text = extract_text_from_pdf(pdf_bytes)
            writer.writerow([key, extracted_text.replace('\n', ' ')])  # pour éviter les sauts de ligne dans CSV

print(f" OCR terminé. Données enregistrées dans : {output_csv_path}")


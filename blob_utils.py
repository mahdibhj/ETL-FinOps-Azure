from azure.storage.blob import BlobServiceClient
import io
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime




def get_from_blob(file):
    # Charger les variables d'environnement depuis le fichier .env
    load_dotenv()
    blob_conn_string = os.getenv("blob_conn_string")
    blob_container_name = os.getenv("blob_container_name")
    connection_string = blob_conn_string
    container_name = blob_container_name
    # Connexion au Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file)
    # Télécharger le contenu du fichier
    stream = io.BytesIO()
    stream.write(blob_client.download_blob().readall())
    stream.seek(0)
    # Charger le fichier dans un DataFrame
    df = pd.read_excel(stream)
    try:
    # Filtrer les enregistrements où 'Date' est égale à la date d'aujourd'hui
        today = datetime.today().date()
        df['Date'] = pd.to_datetime(df['Date']).dt.date  # Convertir la colonne 'Date' en format date seulement
        df = df[df['Date'] != today]  # Garder uniquement les enregistrements dont la date est différente d'aujourd'hui
    except:
        pass
    return df


def save_to_blob(df, file):
    # Charger les variables d'environnement depuis le fichier .env
    load_dotenv()
    blob_conn_string = os.getenv("blob_conn_string")
    blob_container_name = os.getenv("blob_container_name")
    connection_string = blob_conn_string
    container_name = blob_container_name
    blob_name = file
    # Convertir le DataFrame en fichier Excel en mémoire
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)

    output.seek(0)  # Revenir au début du buffer
    # Connexion au Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    # Upload du fichier
    blob_client.upload_blob(output, overwrite=True)
    print(f"Fichier sauvegardé sur Azure Blob Storage : {blob_name}")

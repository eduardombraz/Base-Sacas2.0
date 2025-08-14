import asyncio
from playwright.async_api import async_playwright
import time
from datetime import datetime, timedelta
import os
import shutil
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import zipfile
from gspread_dataframe import set_with_dataframe

DOWNLOAD_DIR = "/tmp/shopee_automation"

def rename_downloaded_file(download_dir, download_path):
    try:
        current_hour = datetime.now().strftime("%H")
        new_file_name = f"TO-Packed{current_hour}.zip"
        new_file_path = os.path.join(download_dir, new_file_name)
        if os.path.exists(new_file_path):
            os.remove(new_file_path)
        shutil.move(download_path, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
        return new_file_path
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        return None

def unzip_and_process_data(zip_path, extract_to_dir):
    try:
        unzip_folder = os.path.join(extract_to_dir, "extracted_files")
        os.makedirs(unzip_folder, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_folder)
        print(f"Arquivo '{os.path.basename(zip_path)}' descompactado.")

        csv_files = [os.path.join(unzip_folder, f) for f in os.listdir(unzip_folder) if f.lower().endswith('.csv')]
        
        if not csv_files:
            print("Nenhum arquivo CSV encontrado no ZIP.")
            shutil.rmtree(unzip_folder)
            return None

        print(f"Lendo e unificando {len(csv_files)} arquivos CSV...")
        all_dfs = [pd.read_csv(file, encoding='utf-8') for file in csv_files]
        df_final = pd.concat(all_dfs, ignore_index=True)

        # --- FILTRAR PELA COLUNA17 (índice 17) para data vigente ---
        agora = datetime.now()
        inicio = agora.replace(hour=6, minute=0, second=0, microsecond=0)
        fim = inicio + timedelta(days=1)

        df_final.iloc[:, 17] = pd.to_datetime(df_final.iloc[:, 17], errors='coerce')
        df_final = df_final[(df_final.iloc[:, 17] >= inicio) & (df_final.iloc[:, 17] < fim]()

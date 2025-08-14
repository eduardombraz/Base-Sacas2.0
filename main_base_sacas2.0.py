import asyncio
from playwright.async_api import async_playwright
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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
        current_hour = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%H")
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

        # --- FILTRAR PELA COLUNA17 (índice 17) com fuso horário ---
        agora = datetime.now(ZoneInfo("America/Sao_Paulo"))

        if agora.hour < 6:
            inicio = agora.replace(hour=6, minute=0, second=0, microsecond=0) - timedelta(days=1)
            fim = agora.replace(hour=6, minute=0, second=0, microsecond=0)
        else:
            inicio = agora.replace(hour=6, minute=0, second=0, microsecond=0)
            fim = agora.replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(days=1)

        # Converte Coluna17 para datetime e aplica fuso horário
        df_final.iloc[:, 17] = pd.to_datetime(df_final.iloc[:, 17], errors='coerce').dt.tz_localize("America/Sao_Paulo")
        df_final = df_final[(df_final.iloc[:, 17] >= inicio) & (df_final.iloc[:, 17] < fim)]
        print(f"Dados filtrados entre {inicio} e {fim}. Total de linhas: {len(df_final)}")
        # ---------------------------------------------------------

        print("Iniciando processamento dos dados...")
        colunas_desejadas = [0, 9, 15, 17, 2]
        df_selecionado = df_final.iloc[:, colunas_desejadas].copy()
        df_selecionado.columns = ['Chave', 'Coluna9', 'Coluna15', 'Coluna17', 'Coluna2']
        contagem = df_selecionado['Chave'].value_counts().reset_index()
        contagem.columns = ['Chave', 'Quantidade']
        agrupado = df_selecionado.groupby('Chave').agg({
            'Coluna9': 'first',
            'Coluna15': 'first',
            'Coluna17': 'first',
            'Coluna2': 'first',
        }).reset_index()
        resultado = pd.merge(agrupado, contagem, on='Chave')
        resultado = resultado[['Chave', 'Coluna9', 'Coluna15', 'Coluna17', 'Quantidade', 'Coluna2']]
        
        print("Processamento de dados concluído com sucesso.")
        shutil.rmtree(unzip_folder)
        return resultado
        
    except Exception as e:
        print(f"Erro ao descompactar ou processar os dados: {e}")
        return None

def update_google_sheet_with_dataframe(df_to_upload):
    if df_to_upload is None or df_to_upload.empty:
        print("Nenhum dado para enviar ao Google Sheets.")
        return

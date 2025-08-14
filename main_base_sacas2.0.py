import os
import zipfile
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests

# --- Configurações ---
DOWNLOAD_DIR = "/tmp/shopee_automation"
ZIP_URL = "URL_DO_SEU_ARQUIVO"
ZIP_NAME = "TO-Packed.zip"

# --- Criar pasta de trabalho ---
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- Baixar o ZIP ---
print("Baixando arquivo...")
response = requests.get(ZIP_URL)
zip_path = os.path.join(DOWNLOAD_DIR, ZIP_NAME)
with open(zip_path, "wb") as f:
    f.write(response.content)
print(f"Download concluído: {zip_path}")

# --- Descompactar ---
with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall(DOWNLOAD_DIR)
print(f"Arquivo '{ZIP_NAME}' descompactado.")

# --- Ler todos os CSVs ---
print("Lendo e unificando arquivos CSV...")
arquivos_csv = [os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR) if f.lower().endswith(".csv")]
df_list = [pd.read_csv(arq, sep=";", encoding="utf-8", dtype=str) for arq in arquivos_csv]
df_final = pd.concat(df_list, ignore_index=True)
print(f"Total de linhas antes do filtro: {len(df_final)}")

# --- Definir intervalo de filtro ---
agora = datetime.now(ZoneInfo("America/Sao_Paulo"))

if agora.hour < 6:
    inicio = (agora - timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
    fim = agora.replace(hour=6, minute=0, second=0, microsecond=0)
else:
    inicio = agora.replace(hour=6, minute=0, second=0, microsecond=0)
    fim = (agora + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)

# --- Converter e ajustar timezone da coluna de data (coluna 17 = índice 16) ---
df_final.iloc[:, 17] = pd.to_datetime(
    df_final.iloc[:, 17],
    errors='coerce',
    dayfirst=True
)

if df_final.iloc[:, 17].dt.tz is None:
    df_final.iloc[:, 17] = df_final.iloc[:, 17].dt.tz_localize("America/Sao_Paulo")
else:
    df_final.iloc[:, 17] = df_final.iloc[:, 17].dt.tz_convert("America/Sao_Paulo")

# --- Aplicar filtro ---
df_final = df_final[
    (df_final.iloc[:, 17] >= inicio) &
    (df_final.iloc[:, 17] < fim)
]

print(f"Dados filtrados entre {inicio} e {fim}. Total de linhas: {len(df_final)}")

# --- Enviar para Google Sheets ---
print("Enviando para Google Sheets...")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS_PATH = "credenciais.json"
SPREADSHEET_ID = "SEU_ID_AQUI"
SHEET_NAME = "Base"

creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_PATH, SCOPES)
gc = gspread.authorize(creds)
worksheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
worksheet.clear()
set_with_dataframe(worksheet, df_final)
print("✅ Dados enviados com sucesso.")

# --- Limpeza ---
for f in os.listdir(DOWNLOAD_DIR):
    os.remove(os.path.join(DOWNLOAD_DIR, f))
print(f"Diretório de trabalho '{DOWNLOAD_DIR}' limpo.")

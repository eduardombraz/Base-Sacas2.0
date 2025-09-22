import asyncio  
from playwright.async_api import async_playwright  
import time  
import datetime  
import os  
import shutil  
import pandas as pd  
import gspread  
from oauth2client.service_account import ServiceAccountCredentials  
import zipfile  
from gspread_dataframe import set_with_dataframe  
import traceback  
import json  
  
# --- CONFIGURAÇÕES ---  
DOWNLOAD_DIR = "/tmp/shopee_automation"  
JSON_CREDS_FILE = "hxh.json"  
GOOGLE_SHEET_NAME = "FIFO INBOUND SP5"  
GOOGLE_SHEET_TAB = "Base"  
# Pega o estado de autenticação do GitHub Secret  
AUTH_STATE_JSON = os.environ.get("AUTH_STATE")  
  
def rename_downloaded_file(download_dir, download_path):  
    """Renomeia o arquivo baixado para incluir a hora atual."""  
    try:  
        current_hour = datetime.datetime.now().strftime("%H")  
        new_file_name = f"SOC_Received_{current_hour}.zip"  
        new_file_path = os.path.join(download_dir, new_file_name)  
        if os.path.exists(new_file_path):  
            os.remove(new_file_path)  
            print(f"Arquivo antigo '{new_file_name}' removido.")  
        shutil.move(download_path, new_file_path)  
        print(f"Arquivo salvo como: {new_file_path}")  
        return new_file_path  
    except FileNotFoundError:  
        print(f"Erro ao renomear: Arquivo de origem '{download_path}' não encontrado.")  
        return None  
    except Exception as e:  
        print(f"Erro ao renomear o arquivo: {e}")  
        return None  
  
def unzip_and_process_data(zip_path, extract_to_dir):  
    """Descompacta um arquivo, unifica os CSVs e processa os dados."""  
    try:  
        unzip_folder = os.path.join(extract_to_dir, "extracted_files")  
        if os.path.exists(unzip_folder):  
            shutil.rmtree(unzip_folder)  
        os.makedirs(unzip_folder, exist_ok=True)  
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:  
            zip_ref.extractall(unzip_folder)  
        print(f"Arquivo '{os.path.basename(zip_path)}' descompactado.")  
        csv_files = [os.path.join(unzip_folder, f) for f in os.listdir(unzip_folder) if f.lower().endswith('.csv')]  
        if not csv_files:  
            print("Nenhum arquivo CSV encontrado no ZIP.")  
            return None  
        print(f"Lendo e unificando {len(csv_files)} arquivos CSV...")  
        all_dfs = [pd.read_csv(file, encoding='utf-8', on_bad_lines='skip') for file in csv_files]  
        df_final = pd.concat(all_dfs, ignore_index=True)  
        print("Iniciando processamento dos dados...")  
        indices_para_manter = [0, 14, 39, 40, 48]  
        df_final = df_final.iloc[:, indices_para_manter]  
        print("Processamento de dados concluído com sucesso.")  
        return df_final  
    except Exception as e:  
        print(f"Erro ao descompactar ou processar os dados: {e}")  
        return None  
    finally:  
        if 'unzip_folder' in locals() and os.path.exists(unzip_folder):  
            shutil.rmtree(unzip_folder)  
  
def update_google_sheet_with_dataframe(df_to_upload):  
    """Atualiza uma Google Sheet com um DataFrame."""  
    if df_to_upload is None or df_to_upload.empty:  
        print("Nenhum dado para enviar ao Google Sheets.")  
        return  
    try:  
        print("Enviando dados processados para o Google Sheets...")  
        df_to_upload = df_to_upload.fillna("").astype(str)  
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive"]  
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_CREDS_FILE, scope)  
        client = gspread.authorize(creds)  
        planilha = client.open(GOOGLE_SHEET_NAME)  
        try:  
            aba = planilha.worksheet(GOOGLE_SHEET_TAB)  
        except gspread.exceptions.WorksheetNotFound:  
            print(f"Aba '{GOOGLE_SHEET_TAB}' não encontrada. Criando uma nova.")  
            aba = planilha.add_worksheet(title=GOOGLE_SHEET_TAB, rows="1000", cols="20")  
        aba.clear()  
        set_with_dataframe(aba, df_to_upload)  
        print("✅ Dados enviados para o Google Sheets com sucesso!")  
    except Exception as e:  
        print(f"❌ Erro ao enviar para o Google Sheets:\n{traceback.format_exc()}")  
  
async def main():  
    if not AUTH_STATE_JSON:  
        print("❌ Erro Crítico: Secret 'AUTH_STATE' não encontrado. Siga as instruções para criar o estado de autenticação.")  
        return  
  
    # Escreve o conteúdo do secret em um arquivo temporário  
    auth_file_path = os.path.join(DOWNLOAD_DIR, "auth_state.json")  
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)  
    with open(auth_file_path, "w") as f:  
        f.write(AUTH_STATE_JSON)  
  
    browser = None  
    try:  
        async with async_playwright() as p:  
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])  
            # Carrega o estado de autenticação salvo no contexto do navegador  
            context = await browser.new_context(storage_state=auth_file_path, accept_downloads=True, viewport={"width": 1920, "height": 1080})  
            page = await context.new_page()  
  
            print("Estado de autenticação carregado. Navegando diretamente para a página de rastreamento...")  
            await page.goto("https://spx.shopee.com.br/#/orderTracking", timeout=60000)  
  
            print("Aguardando página de rastreamento carregar (verificando botão Exportar)...")  
            export_button = page.get_by_role('button', name='Exportar')  
            await export_button.wait_for(state="visible", timeout=60000)  
            print("Página de rastreamento carregada com sucesso.")  
  
            print("Configurando filtros para exportação...")  
            await export_button.click()  
              
            await page.locator('label:has-text("Status do pedido") + div').click()  
            await page.get_by_role("treeitem", name="SOC_Received").click()  
            await page.locator('label:has-text("SoC") + div').click()  
            await page.get_by_role('textbox', name='procurar por').fill('SoC_SP_Cravinhos')  
            await page.get_by_role('listitem', name='SoC_SP_Cravinhos').click()  
              
            await page.get_by_role("button", name="Confirmar").click()  
            print("Aguardando o sistema processar o relatório. Isso pode levar vários minutos...")  
  
            download_button = page.get_by_role("button", name="Baixar").first  
            await download_button.wait_for(state="visible", timeout=600000)  
            print("Relatório pronto. Iniciando download.")  
  
            async with page.expect_download() as download_info:  
                await download_button.click()  
              
            download = await download_info.value  
            download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)  
            await download.save_as(download_path)  
            print(f"Download concluído: {download_path}")  
  
            renamed_zip_path = rename_downloaded_file(DOWNLOAD_DIR, download_path)  
            if renamed_zip_path:  
                final_dataframe = unzip_and_process_data(renamed_zip_path, DOWNLOAD_DIR)  
                update_google_sheet_with_dataframe(final_dataframe)  
  
    except Exception as e:  
        print(f"❌ Erro durante o processo principal: {e}")  
        print(traceback.format_exc())  
    finally:  
        if browser:  
            await browser.close()  
            print("Navegador fechado.")  
        if os.path.exists(DOWNLOAD_DIR):  
            shutil.rmtree(DOWNLOAD_DIR)  
            print(f"Diretório de trabalho '{DOWNLOAD_DIR}' limpo.")  
  
if __name__ == "__main__":  
    asyncio.run(main())

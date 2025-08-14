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
# from gspread_dataframe import set_with_dataframe  # This is no longer needed

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

        # --- CONVERSÃO DE DATETIME SEM TIMEZONE (naive) ---
        # Convert column to datetime, handling potential errors and date formats
        df_final.iloc[:, 17] = pd.to_datetime(df_final.iloc[:, 17], errors='coerce')

        agora = datetime.now()  # agora é naive
        if agora.hour < 6:
            inicio = (agora - timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
            fim = agora.replace(hour=6, minute=0, second=0, microsecond=0)
        else:
            inicio = agora.replace(hour=6, minute=0, second=0, microsecond=0)
            fim = (agora + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)

        # FILTRA APENAS OS DADOS NO INTERVALO CORRETO
        # Ensure the column is not timezone-aware before comparison with naive datetime
        df_final = df_final[df_final.iloc[:, 17].dt.tz_localize(None).between(inicio, fim, inclusive='left')]
        print(f"Dados filtrados entre {inicio} e {fim}. Total de linhas: {len(df_final)}")

        if df_final.empty:
            print("Nenhuma linha encontrada no intervalo de tempo especificado após a filtragem.")
            shutil.rmtree(unzip_folder)
            return None

        print("Iniciando processamento dos dados...")
        colunas_desejadas = [0, 9, 15, 17, 2]
        df_selecionado = df_final.iloc[:, colunas_desejadas].copy()
        df_selecionado.columns = ['Chave', 'Coluna9', 'Coluna15', 'Coluna17', 'Coluna2']

        # Ensure datetime column is formatted correctly for Google Sheets
        df_selecionado['Coluna17'] = df_selecionado['Coluna17'].dt.strftime('%Y-%m-%d %H:%M:%S')

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

        shutil.rmtree(unzip_folder)
        return resultado
        
    except Exception as e:
        print(f"Erro ao descompactar ou processar os dados: {e}")
        return None

# =========== MODIFIED FUNCTION ===========
def update_google_sheet_with_dataframe(df_to_upload):
    if df_to_upload is None or df_to_upload.empty:
        print("Nenhum dado para enviar ao Google Sheets.")
        return
        
    try:
        print("Enviando dados processados para o Google Sheets...")
        scope = ["https://spreadsheets.google.com/feeds", 
                 'https://www.googleapis.com/auth/spreadsheets', 
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)
        client = gspread.authorize(creds)
        
        planilha = client.open("Base Sacas")
        aba = planilha.worksheet("Base")
        
        # Clear the sheet first
        aba.clear()
        
        # Convert the DataFrame to a list of lists, including the header
        # This is the format the gspread's native update method expects
        values_to_upload = [df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist()
        
        # Update the sheet starting at cell A1 with all the data
        aba.update('A1', values_to_upload)
        
        print("✅ Dados enviados para o Google Sheets com sucesso!")
        time.sleep(5)

    except Exception as e:
        print(f"❌ Erro ao enviar para o Google Sheets: {e}")
# =========================================

async def main():
    # Make sure the download directory is clean before starting
    if os.path.exists(DOWNLOAD_DIR):
        shutil.rmtree(DOWNLOAD_DIR)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False,
                                          args=["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"])
        context = await browser.new_context(
            accept_downloads=True, 
            viewport={"width": 1920, "height": 1080},
            # Set the download path directly in the context
            downloads_path=DOWNLOAD_DIR
        )
        page = await context.new_page()
        try:
            agora = datetime.now()
            if agora.hour < 6:
                d1 = (agora - timedelta(days=1)).strftime("%Y/%m/%d 06:00")
                d0 = agora.strftime("%Y/%m/%d 06:00")
            else:
                d1 = agora.strftime("%Y/%m/%d 06:00")
                d0 = (agora + timedelta(days=1)).strftime("%Y/%m/%d 06:00")

            # LOGIN
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=15000)
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill('Ops71223')
            await page.locator('xpath=//*[@placeholder="Senha"]').fill('@Shopee123')
            await page.locator('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button').click()
            await page.wait_for_load_state('networkidle', timeout=20000) # Wait for network to be idle
            
            # Better way to handle pop-ups
            try:
                await page.locator('.ssc-dialog-close').click(timeout=5000)
                print("Pop-up de diálogo fechado.")
            except Exception:
                print("Nenhum pop-up de diálogo foi encontrado ou ele desapareceu.")
            
            # NAVEGAÇÃO E EXPORT
            await page.goto("https://spx.shopee.com.br/#/general-to-management")
            await page.wait_for_selector('role=button[name="Exportar"]', timeout=15000)
            await page.get_by_role("button", name="Exportar").click()
            
            # Wait for the export modal to be visible
            await page.wait_for_selector('div.ssc-dialog-body', timeout=10000)
            
            # Preencher datas no site
            date_inputs = page.locator('xpath=//*[@placeholder="Please choose date"]')
            
            await date_inputs.nth(0).fill(d1)
            await page.keyboard.press("Escape") # Close calendar pop-up
            await date_inputs.nth(1).fill(d0)
            await page.keyboard.press("Escape") # Close calendar pop-up
            
            await page.get_by_text("Criado em", exact=True).click()
            # Wait for the next section to appear after clicking
            await page.wait_for_selector(".s-tree-node__content", timeout=5000)
            await page.locator(".s-tree-node__content > .ssc-checkbox-wrapper > .ssc-checkbox > .ssc-checkbox-input").first.click()
            await page.get_by_role("button", name="Confirmar").click()

            # Wait for the "Baixar" (Download) button to become enabled/visible after confirmation
            await page.wait_for_selector('role=button[name="Baixar"]', timeout=360000)
            
            # DOWNLOAD
            async with page.expect_download() as download_info:
                await page.get_by_role("button", name="Baixar").first.click()
            
            download = await download_info.value
            # The file is already saved in DOWNLOAD_DIR due to context setting
            download_path = await download.path()
            print(f"Download concluído: {download_path}")
            
            # Rename using the suggested filename to keep it consistent
            final_download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            shutil.move(download_path, final_download_path)

            renamed_zip_path = rename_downloaded_file(DOWNLOAD_DIR, final_download_path)
            if renamed_zip_path:
                final_dataframe = unzip_and_process_data(renamed_zip_path, DOWNLOAD_DIR)
                update_google_sheet_with_dataframe(final_dataframe)

        except Exception as e:
            print(f"Erro durante o processo principal: {e}")
        finally:
            await browser.close()
            if os.path.exists(DOWNLOAD_DIR):
                shutil.rmtree(DOWNLOAD_DIR)
                print(f"Diretório de trabalho '{DOWNLOAD_DIR}' limpo.")

if __name__ == "__main__":
    asyncio.run(main())

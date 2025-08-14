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

DOWNLOAD_DIR = "/tmp/shopee_automation"

def unzip_and_process_data(zip_path, extract_to_dir):
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
            shutil.rmtree(unzip_folder)
            return None

        print(f"Lendo e unificando {len(csv_files)} arquivos CSV...")
        all_dfs = [pd.read_csv(file, encoding='utf-8') for file in csv_files]
        df_final = pd.concat(all_dfs, ignore_index=True)

        df_final.iloc[:, 17] = pd.to_datetime(df_final.iloc[:, 17], errors='coerce')

        agora = datetime.now()
        if agora.hour < 6:
            inicio = (agora - timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
            fim = agora.replace(hour=6, minute=0, second=0, microsecond=0)
        else:
            inicio = agora.replace(hour=6, minute=0, second=0, microsecond=0)
            fim = (agora + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)

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
        if 'unzip_folder' in locals() and os.path.exists(unzip_folder):
            shutil.rmtree(unzip_folder)
        return None

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
        
        aba.clear()
        
        values_to_upload = [df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist()
        
        aba.update('A1', values_to_upload, value_input_option='USER_ENTERED')
        
        print("✅ Dados enviados para o Google Sheets com sucesso!")
        time.sleep(5)

    except Exception as e:
        print(f"❌ Erro ao enviar para o Google Sheets: {e}")

async def main():
    if os.path.exists(DOWNLOAD_DIR):
        shutil.rmtree(DOWNLOAD_DIR)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False,
                                          args=["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"])
        
        context = await browser.new_context(
            accept_downloads=True, 
            viewport={"width": 1920, "height": 1080}
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

            print("Iniciando login...")
            await page.goto("https://spx.shopee.com.br/")
            await page.get_by_placeholder("Ops ID").fill('Ops71223')
            await page.get_by_placeholder("Senha").fill('@Shopee123')
            await page.get_by_role("button", name="Entrar").click()
            await page.wait_for_load_state('networkidle', timeout=30000)
            print("Login realizado com sucesso.")

            try:
                await page.locator('.ssc-dialog-close').click(timeout=5000)
                print("Pop-up de diálogo fechado.")
            except Exception:
                print("Nenhum pop-up de diálogo foi encontrado.")
            
            # --- SEÇÃO CORRIGIDA ---
            print("Navegando para a página de gerenciamento...")
            await page.goto("https://spx.shopee.com.br/#/general-to-management")

            # Espera o botão 'Exportar' estar pronto antes de clicar
            export_button = page.get_by_role("button", name="Exportar")
            await export_button.wait_for(state="visible", timeout=15000)
            await export_button.click()

            # CORREÇÃO: Espera por um elemento específico DENTRO do pop-up.
            # Isto é muito mais confiável do que esperar por um 'div' genérico.
            print("Esperando o pop-up de exportação aparecer...")
            first_date_input = page.get_by_placeholder("Please choose date").first
            await first_date_input.wait_for(state="visible", timeout=20000)

            print("Preenchendo o formulário de exportação...")
            date_inputs = page.get_by_placeholder("Please choose date")
            await date_inputs.nth(0).fill(d1)
            await page.keyboard.press("Escape")
            await date_inputs.nth(1).fill(d0)
            await page.keyboard.press("Escape")
            # --- FIM DA SEÇÃO CORRIGIDA ---
            
            await page.get_by_text("Criado em", exact=True).click()
            await page.locator(".s-tree-node__content > .ssc-checkbox-wrapper > .ssc-checkbox > .ssc-checkbox-input").first.click()
            await page.get_by_role("button", name="Confirmar").click()

            await page.wait_for_selector('role=button[name="Baixar"]', timeout=360000)
            print("Formulário preenchido. Iniciando o download...")

            async with page.expect_download() as download_info:
                await page.get_by_role("button", name="Baixar").first.click()
            
            download = await download_info.value
            
            current_hour = datetime.now().strftime("%H")
            final_zip_path = os.path.join(DOWNLOAD_DIR, f"TO-Packed{current_hour}.zip")
            await download.save_as(final_zip_path)
            
            print(f"Download concluído e arquivo salvo como: {final_zip_path}")

            if os.path.exists(final_zip_path):
                final_dataframe = unzip_and_process_data(final_zip_path, DOWNLOAD_DIR)
                update_google_sheet_with_dataframe(final_dataframe)

        except Exception as e:
            screenshot_path = "error_screenshot.png"
            await page.screenshot(path=screenshot_path)
            print(f"❌ Erro durante o processo principal: {e}")
            print(f"📷 Screenshot salvo em: {screenshot_path}")

        finally:
            await browser.close()
            if os.path.exists(DOWNLOAD_DIR):
                shutil.rmtree(DOWNLOAD_DIR)
                print(f"Diretório de trabalho '{DOWNLOAD_DIR}' limpo.")

if __name__ == "__main__":
    asyncio.run(main())

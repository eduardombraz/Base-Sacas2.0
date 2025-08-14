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
        print(f"Total de linhas antes do filtro: {len(df_final)}")

        # --- FILTRAR PELA COLUNA17 (índice 17) com fuso horário ---
        agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
        if agora.hour < 6:
            inicio = (agora - timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
            fim = agora.replace(hour=6, minute=0, second=0, microsecond=0)
        else:
            inicio = agora.replace(hour=6, minute=0, second=0, microsecond=0)
            fim = (agora + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)

        inicio = inicio.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
        fim = fim.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))

        # Converte Coluna17 para datetime com segurança
        df_final.iloc[:, 17] = pd.to_datetime(df_final.iloc[:, 17], errors='coerce', dayfirst=True)

        # Mantém apenas linhas válidas e copia
        df_final = df_final[df_final.iloc[:, 17].notna()].copy()

        # Força datetimeindex e aplica fuso horário
        df_final.iloc[:, 17] = pd.DatetimeIndex(df_final.iloc[:, 17]).tz_localize("America/Sao_Paulo", ambiguous='NaT', nonexistent='NaT')
        print(f"Linhas após remover valores inválidos na coluna17: {len(df_final)}")

        # Filtra pelo período desejado
        df_final = df_final[(df_final.iloc[:, 17] >= inicio) & (df_final.iloc[:, 17] < fim)]
        print(f"Linhas após filtro de data entre {inicio} e {fim}: {len(df_final)}")

        # ---------------------------------------------------------
        print("Iniciando processamento dos dados...")
        colunas_desejadas = [0, 9, 15, 17, 2]
        df_selecionado = df_final.iloc[:, colunas_desejadas].copy()
        df_selecionado.columns = ['Chave', 'Coluna9', 'Coluna15', 'Coluna17', 'Coluna2']

        # Contagem de ocorrências por chave
        contagem = df_selecionado['Chave'].value_counts().reset_index()
        contagem.columns = ['Chave', 'Quantidade']

        # Agrupando para retornar apenas uma linha por chave
        agrupado = df_selecionado.groupby('Chave', as_index=False).agg({
            'Coluna9': 'first',
            'Coluna15': 'first',
            'Coluna17': 'first',
            'Coluna2': 'first',
        })
        print(f"Linhas após agrupamento por chave: {len(agrupado)}")

        # Merge para incluir quantidade
        resultado = pd.merge(agrupado, contagem, on='Chave')
        resultado = resultado[['Chave', 'Coluna9', 'Coluna15', 'Coluna17', 'Quantidade', 'Coluna2']]

        # --- CONVERSÃO SEGURA DA COLUNA17 PARA REMOVER FUSO HORÁRIO ---
        resultado['Coluna17'] = pd.to_datetime(resultado['Coluna17'], errors='coerce')
        resultado['Coluna17'] = resultado['Coluna17'].dt.tz_localize(None)

        # Mostra as 5 primeiras linhas antes do envio
        print("5 primeiras linhas do DataFrame final:")
        print(resultado.head())

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
    try:
        print("Enviando dados processados para o Google Sheets...")
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name("hxh.json", scope)
        client = gspread.authorize(creds)
        planilha = client.open("Base Sacas")
        aba = planilha.worksheet("Base")

        # Limpa a aba antes de escrever
        aba.clear()

        # Envia o DataFrame
        set_with_dataframe(aba, df_to_upload, include_index=False, include_column_header=True)

        print("✅ Dados enviados para o Google Sheets com sucesso!")

    except Exception as e:
        print(f"❌ Erro real ao enviar para o Google Sheets: {e}")

async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"]
        )
        context = await browser.new_context(accept_downloads=True, viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        try:
            # LOGIN
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=15000)
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill('Ops71223')
            await page.locator('xpath=//*[@placeholder="Senha"]').fill('@Shopee123')
            await page.locator('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button').click()
            await page.wait_for_timeout(15000)

            try:
                await page.locator('.ssc-dialog-close').click(timeout=5000)
            except:
                print("Nenhum pop-up de diálogo foi encontrado.")
            await page.keyboard.press("Escape")

            # NAVEGAÇÃO E EXPORT
            await page.goto("https://spx.shopee.com.br/#/general-to-management")
            await page.wait_for_timeout(8000)
            await page.get_by_role("button", name="Exportar").click()
            await page.wait_for_timeout(8000)
            await page.locator('xpath=/html[1]/body[1]/span[4]/div[1]/div[1]/div[1]').click()
            await page.wait_for_timeout(8000)
            await page.locator(".s-tree-node__content > .ssc-checkbox-wrapper > .ssc-checkbox > .ssc-checkbox-input").first.click()
            await page.wait_for_timeout(8000)
            await page.get_by_role("button", name="Confirmar").click()
            await page.wait_for_timeout(360000)  # espera do download

            # DOWNLOAD
            async with page.expect_download() as download_info:
                await page.get_by_role("button", name="Baixar").first.click()
            download = await download_info.value
            download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path)
            print(f"Download concluído: {download_path}")

            renamed_zip_path = rename_downloaded_file(DOWNLOAD_DIR, download_path)
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

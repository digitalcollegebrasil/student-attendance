import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import os

load_dotenv()

credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

if not credentials_json:
    raise ValueError("A variável de ambiente 'GOOGLE_CREDENTIALS_JSON' não está definida.")

creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_json, scope)
client = gspread.authorize(creds)

GOOGLE_SHEET_ID = '1OAc-A6bJ0J1wRz-mnv-BVtOH9V93Vk_bs43Edhy8-fc'
sheet_destino = client.open_by_key(GOOGLE_SHEET_ID).sheet1

cabecalho = sheet_destino.row_values(1)
linhas_destino = sheet_destino.get_all_values()

if creds:
    print("Credenciais carregadas com sucesso.")

for row in linhas_destino[1:6]:
    print(row)
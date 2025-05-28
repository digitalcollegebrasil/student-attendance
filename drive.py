import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credenciais.json', scope)
client = gspread.authorize(creds)

df_origem = pd.read_excel('combined_data.xlsx')
linhas_origem = df_origem.values.tolist()

GOOGLE_SHEET_ID = 'SEU_ID_AQUI'
sheet_destino = client.open_by_key(GOOGLE_SHEET_ID).sheet1

cabecalho = sheet_destino.row_values(1)
linhas_destino = sheet_destino.get_all_values()

def atualizar_planilha():
    for nova_linha in linhas_origem:
        data = str(nova_linha[0])
        turma = str(nova_linha[1])

        encontrada = False

        for idx, linha_existente in enumerate(linhas_destino[1:], start=2):
            if linha_existente[0] == data and linha_existente[1] == turma:
                sheet_destino.delete_row(idx)
                sheet_destino.insert_row(nova_linha, idx)
                print(f"Atualizado: {data} - {turma}")
                encontrada = True
                break

        if not encontrada:
            sheet_destino.append_row(nova_linha)
            print(f"Inserido: {data} - {turma}")

atualizar_planilha()

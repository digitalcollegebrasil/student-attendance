import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from datetime import datetime

# === CONFIGURAÇÕES ===
credentials_json = "credentials.json"
GOOGLE_SHEET_ID = "19_bvzaFfHkHWlRi4dV7hEJ44W2LoJIOSJkWeWW7CQ4A"

# === AUTENTICAÇÃO ===
scope_rw = ["https://www.googleapis.com/auth/spreadsheets"]
scope_ro = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds_rw = ServiceAccountCredentials.from_json_keyfile_name(credentials_json, scope_rw)
creds_ro = ServiceAccountCredentials.from_json_keyfile_name(credentials_json, scope_ro)

client = gspread.authorize(creds_rw)
service_ro = build("sheets", "v4", credentials=creds_ro)
service_rw = build("sheets", "v4", credentials=creds_rw)

sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1  # primeira aba

# === TIPOS IDEAIS ===
tipos_ideais = {
    1: "DATE",
    2: "STRING",
    3: "STRING",
    4: "STRING",
    5: "NUMBER",
    6: "NUMBER",
    7: "NUMBER",
    8: "STRING",
    9: "NUMBER",
    10: "NUMBER",
    11: "STRING",
    12: "STRING"
}

# === PASSO 1: Detectar linhas erradas ===
result = service_ro.spreadsheets().get(
    spreadsheetId=GOOGLE_SHEET_ID,
    includeGridData=True
).execute()

rows = result["sheets"][0]["data"][0]["rowData"]
linhas_erradas = []

for r_idx, row in enumerate(rows, start=1):
    if r_idx == 1:  # pula cabeçalho
        continue
    if "values" not in row:
        continue

    erros = []
    for c_idx, cell in enumerate(row["values"], start=1):
        user_value = cell.get("userEnteredValue", {})
        effective_value = cell.get("effectiveValue", {})
        number_format = cell.get("userEnteredFormat", {}).get("numberFormat", {})

        if "numberValue" in effective_value:
            tipo = number_format.get("type", "NUMBER")
            if number_format.get("type") == "DATE":
                tipo = "DATE"
        elif "stringValue" in effective_value:
            tipo = "STRING"
        elif "boolValue" in effective_value:
            tipo = "BOOLEAN"
        elif "formulaValue" in user_value:
            tipo = "FORMULA"
        else:
            tipo = "VAZIO"

        if c_idx in tipos_ideais:
            if tipo != tipos_ideais[c_idx] and tipo != "VAZIO":
                erros.append((c_idx, tipo))

    if erros:
        print(f"⚠️ Linha {r_idx} divergente → {erros}")
        linhas_erradas.append(r_idx)

# === PASSO 2: Corrigir apenas linhas erradas ===
def corrigir_linhas(sheet_destino, linhas_alvo):
    valores_existentes = sheet_destino.get_all_values()
    updates = []

    for idx in linhas_alvo:
        linha = valores_existentes[idx-1]
        data = linha[0]
        numero = linha[8] if len(linha) >= 9 else None

        valores_corrigidos = linha.copy()
        update_needed = False

        # Corrigir data (coluna 1 → deve virar string no formato dd/MM/yyyy)
        if data:
            try:
                dt = datetime.strptime(data, "%d/%m/%Y")
                valores_corrigidos[0] = dt.strftime("%d/%m/%Y")
                update_needed = True
            except:
                pass

        # Corrigir número (coluna 9 → deve virar número de verdade)
        if numero and numero.isdigit():
            valores_corrigidos[8] = int(numero)
            update_needed = True

        if update_needed:
            print(f"🔧 Preparando correção linha {idx}...")
            updates.append((idx, valores_corrigidos))

    if updates:
        data = {
            "valueInputOption": "USER_ENTERED",  # força Sheets interpretar
            "data": []
        }
        for idx, valores_corrigidos in updates:
            data["data"].append({
                "range": f"A{idx}:Z{idx}",
                "values": [valores_corrigidos]
            })

        service_rw.spreadsheets().values().batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body=data
        ).execute()

        print("✅ Correções aplicadas com tipagem do Google Sheets")

# === PASSO 3: Forçar formatação da coluna A como DATE ===
def formatar_datas():
    requests = [{
        "repeatCell": {
            "range": {
                "sheetId": sheet._properties['sheetId'],  # id da aba atual
                "startColumnIndex": 0,  # Coluna A
                "endColumnIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": "DATE",
                        "pattern": "dd/MM/yyyy"
                    }
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    }]

    service_rw.spreadsheets().batchUpdate(
        spreadsheetId=GOOGLE_SHEET_ID,
        body={"requests": requests}
    ).execute()

    print("📅 Coluna A formatada como DATE (dd/MM/yyyy)")

# === EXECUTAR ===
if linhas_erradas:
    corrigir_linhas(sheet, linhas_erradas)
    formatar_datas()
    print("✅ Linhas corrigidas e formatação aplicada!")
else:
    print("✅ Nenhuma linha precisa de correção")

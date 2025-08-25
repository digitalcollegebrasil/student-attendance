import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

# === CONFIGURAÇÕES ===
credentials_json = "credentials.json"
GOOGLE_SHEET_ID = "1OAc-A6bJ0J1wRz-mnv-BVtOH9V93Vk_bs43Edhy8-fc"

# === AUTENTICAÇÃO ===
scope_ro = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds_ro = ServiceAccountCredentials.from_json_keyfile_name(credentials_json, scope_ro)
service_ro = build("sheets", "v4", credentials=creds_ro)

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

# === CAPTURA DADOS ===
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

if not linhas_erradas:
    print("✅ Todas as linhas estão com tipagem correta.")

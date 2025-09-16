import os
import time
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, date
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

load_dotenv()

email_address = os.getenv("SPONTE_EMAIL")
password_value = os.getenv("SPONTE_PASSWORD")
credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")

current_dir = os.path.dirname(__file__)
COMBINED_PATH = os.path.join(current_dir, 'combined_data.xlsx')

# ============ PARÂMETROS ============
# lista de destinatários e cc (ajuste como quiser)
DESTINATARIOS = [
    "cauan.victor@engajacomunicacao.com.br",
]
CC = [
    "cauan.victor@engajacomunicacao.com.br",
]

# opcional: quantos dias olhar pra trás (por padrão, pega tudo)
REPORT_DAYS = int(os.getenv("REPORT_DAYS", "0"))

# ============ FILTRO 100% PRESENÇA ============
def construir_relatorio_100(df_base: pd.DataFrame) -> pd.DataFrame:
    cols = set(df_base.columns)

    col_freq  = "Frequente" if "Frequente" in cols else ("Frequentes" if "Frequentes" in cols else None)
    col_nfreq = "Não Frequentes" if "Não Frequentes" in cols else ("NaoFrequente" if "NaoFrequente" in cols else None)
    col_turma = "Turma" if "Turma" in cols else ("Nome" if "Nome" in cols else None)

    obrigatorias = {
        "Integrantes": "Integrantes",
        "Frequente": col_freq,
        "Não Frequentes": col_nfreq,
        "Data": "Data",
    }
    faltando = [k for k, v in obrigatorias.items() if v is None or v not in cols]
    if faltando:
        raise KeyError(f"Colunas obrigatórias ausentes: {faltando} — tenho {sorted(cols)}")

    for c in ["Integrantes", col_freq, col_nfreq]:
        df_base[c] = pd.to_numeric(df_base[c], errors="coerce").fillna(0)

    df_base["Data_dt"] = pd.to_datetime(df_base["Data"], dayfirst=True, errors="coerce")

    if REPORT_DAYS > 0:
        limite = pd.Timestamp.now(tz="America/Fortaleza").normalize() - pd.Timedelta(days=REPORT_DAYS)
        df_base = df_base[df_base["Data_dt"] >= limite]

    mask = (df_base[col_nfreq] == 0) & (df_base[col_freq] == df_base["Integrantes"]) & (df_base["Integrantes"] > 0)

    keep_cols = ["Data_dt", col_turma, "Curso", "Professor", "Integrantes", "Horario", "Sede"]
    keep_cols = [c for c in keep_cols if c in df_base.columns]

    df100 = df_base.loc[mask, keep_cols].copy()

    # padroniza nome "Turma" na saída
    if col_turma != "Turma" and col_turma in df100.columns:
        df100.rename(columns={col_turma: "Turma"}, inplace=True)

    df100 = df100.sort_values(["Data_dt", "Sede", "Turma"], na_position="last").reset_index(drop=True)
    return df100

def _norm_val(valor, coluna_nome, colunas_numericas):
    # vazios
    if valor is None or (isinstance(valor, float) and pd.isna(valor)) or (isinstance(valor, str) and valor.strip()==""):
        return ""

    # Data -> dd/MM/yyyy
    if coluna_nome == "Data":
        if isinstance(valor, (pd.Timestamp, datetime, date)):
            # garante string dd/MM/yyyy
            if isinstance(valor, datetime):
                valor = valor.date()
            return valor.strftime("%d/%m/%Y")
        # tenta converter string para data
        dt = pd.to_datetime(str(valor), dayfirst=True, errors="coerce")
        return dt.strftime("%d/%m/%Y") if pd.notna(dt) else str(valor)

    # Numéricas -> número (int/float) sem aspas
    if coluna_nome in colunas_numericas:
        num = pd.to_numeric(str(valor).replace(",", ".").strip(), errors="coerce")
        if pd.isna(num):
            return ""
        return int(num) if float(num).is_integer() else float(num)

    # Demais -> string
    return str(valor)

input_file = COMBINED_PATH
df = pd.read_excel(input_file)

df.columns = [c.strip() for c in df.columns]
df.rename(columns={
    "Nome": "Turma",
    "Frequentes": "Frequente",
    "NaoFrequente": "Não Frequentes",
    "DiasSemana": "Dias da Semana",
    "Data Início": "DataInicio",
}, inplace=True)

df_100 = construir_relatorio_100(df)

# salva um anexo com o relatório
anexo_path = os.path.join(current_dir, "turmas_100_presenca.xlsx")
if not df_100.empty:
    temp_to_save = df_100.copy()
    temp_to_save["Data"] = temp_to_save["Data_dt"].dt.strftime("%d/%m/%Y")
    temp_to_save.drop(columns=["Data_dt"], inplace=True)
    temp_to_save.to_excel(anexo_path, index=False)
else:
    # se quiser mesmo assim gerar anexo vazio
    pd.DataFrame(columns=["Data", "Turma", "Curso", "Professor", "Integrantes", "Horario", "Sede"]).to_excel(anexo_path, index=False)

# ============ E-MAIL (SMTP GMAIL) ============
def enviar_relatorio_turmas_100(df100: pd.DataFrame, to_list, cc_list=None):
    sender_email = os.getenv("EMAIL_USER")
    password = os.getenv("EMAIL_PASSWORD")
    if not sender_email or not password:
        raise RuntimeError("Defina EMAIL_USER e EMAIL_PASSWORD nas variáveis de ambiente.")

    hoje_brt = pd.Timestamp.now(tz="America/Fortaleza")
    if df100.empty:
        assunto = f"[Relatório] Turmas 100% presença — nenhum registro ({hoje_brt:%d/%m/%Y})"
        corpo_html = f"""
        <p>Olá,</p>
        <p>Não foram encontradas turmas com <strong>100% de presença</strong> no período considerado.</p>
        <p>Data de geração: <strong>{hoje_brt:%d/%m/%Y %H:%M}</strong></p>
        """
    else:
        ultimo_dia = df100["Data_dt"].max()
        assunto = f"[Relatório] Turmas 100% presença — até {ultimo_dia:%d/%m/%Y}"
        # tabela HTML
        tbl = df100.copy()
        tbl["Data"] = tbl["Data_dt"].dt.strftime("%d/%m/%Y")
        tbl = tbl[["Data", "Sede", "Turma", "Curso", "Professor", "Integrantes", "Horario"]]
        tabela_html = tbl.to_html(index=False, border=0, justify="left")
        corpo_html = f"""
        <p>Olá,</p>
        <p>Segue abaixo o relatório de turmas com <strong>100% de presença</strong> (sem faltas):</p>
        {tabela_html}
        <p>Anexo: <em>turmas_100_presenca.xlsx</em></p>
        <p>Gerado em: <strong>{hoje_brt:%d/%m/%Y %H:%M}</strong></p>
        """

    # monta mensagem
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = assunto
    msg.attach(MIMEText(corpo_html, "html"))

    # anexo
    if os.path.exists(anexo_path):
        with open(anexo_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(anexo_path))
            part["Content-Disposition"] = f'attachment; filename="{os.path.basename(anexo_path)}"'
            msg.attach(part)

    # envio
    all_rcpts = list(to_list) + (cc_list or [])
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, all_rcpts, msg.as_string())

    print(f"📧 E-mail enviado para: {all_rcpts}")

enviar_relatorio_turmas_100(df_100, DESTINATARIOS, CC)

df.rename(columns={
    "Nome": "Turma",
    "Frequentes": "Frequente",
}, inplace=True)

df = df[~df['Turma'].astype(str).str.startswith('GT')]

colunas_numericas = ['Vagas', 'Integrantes', 'Trancados', 'Frequente', 'Não Frequentes']
for coluna in colunas_numericas:
    df[coluna] = pd.to_numeric(df[coluna], errors='coerce')

if 'Turma' not in df.columns or 'Data' not in df.columns:
    print("Colunas 'Turma' e 'Data' são necessárias.")
    exit()

df_online = df[df['Turma'].astype(str).str[2].str.upper() == 'L']
df_presencial = df[df['Turma'].astype(str).str[2].str.upper() != 'L']

# === AUTENTICAÇÃO ===
scope_rw = ["https://www.googleapis.com/auth/spreadsheets"]
scope_ro = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

credentials_raw = os.getenv("GOOGLE_CREDENTIALS_JSON")

def _try_paths():
    # candidatos comuns para fallback
    candidates = [
        os.path.join(current_dir, "credentials.json"),
        os.path.join(current_dir, "service-account.json"),
        os.path.expanduser("~/.credentials/credentials.json"),
        os.path.expanduser("~/.credentials/service-account.json"),
    ]
    # evita duplicados mantendo ordem
    seen, uniq = set(), []
    for c in candidates:
        c = os.path.abspath(c)
        if c not in seen:
            seen.add(c); uniq.append(c)
    return [p for p in uniq if os.path.exists(p)]

def build_creds_any(scopes):
    """
    1) Se GOOGLE_CREDENTIALS_JSON vier com JSON inline -> usa from_json_keyfile_dict
    2) Se vier com caminho -> usa from_json_keyfile_name
    3) Caso contrário -> tenta arquivos locais (credentials.json, etc.)
    """
    # Caso 1: env contém JSON inline
    if credentials_raw and credentials_raw.strip().startswith("{"):
        try:
            cred_dict = json.loads(credentials_raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON parece JSON mas falhou ao parsear: {e}")
        return ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scopes)

    # Caso 2: env contém caminho para o arquivo
    if credentials_raw and credentials_raw.strip():
        cred_path = os.path.abspath(credentials_raw.strip())
        if not os.path.exists(cred_path):
            raise FileNotFoundError(f"Caminho de credenciais não existe: {cred_path}")
        return ServiceAccountCredentials.from_json_keyfile_name(cred_path, scopes)

    # Caso 3: fallback para arquivos locais conhecidos
    tried = []
    for path in _try_paths():
        try:
            return ServiceAccountCredentials.from_json_keyfile_name(path, scopes)
        except Exception as e:
            tried.append(f"{path} -> {e}")

    # Se chegou aqui, nada funcionou
    hints = "\n".join(tried) if tried else "Nenhum arquivo candidato encontrado."
    raise RuntimeError(
        "Não encontrei GOOGLE_CREDENTIALS_JSON e o fallback para credentials.json falhou.\n"
        "Defina a env com o CAMINHO do arquivo ou o CONTEÚDO JSON, "
        "ou coloque um credentials.json ao lado do script.\n"
        f"Tentativas:\n{hints}"
    )

# Cria credenciais RW/RO
creds_rw = build_creds_any(scope_rw)
creds_ro = build_creds_any(scope_ro)

# Clientes
client = gspread.authorize(creds_rw)
service_ro = build("sheets", "v4", credentials=creds_ro)
service_rw = build("sheets", "v4", credentials=creds_rw)

GOOGLE_SHEET_ID = '1OAc-A6bJ0J1wRz-mnv-BVtOH9V93Vk_bs43Edhy8-fc'
sheet = client.open_by_key(GOOGLE_SHEET_ID)
sheet_presencial = sheet.get_worksheet(0)
sheet_online = sheet.get_worksheet(1)

def atualizar_linhas(sheet_destino, df_novos):
    valores_existentes = sheet_destino.get_all_values()

    if len(valores_existentes) < 2:
        print("A planilha precisa ter ao menos duas linhas de cabeçalho.")
        return

    cabecalho = valores_existentes[0]
    dados_existentes = valores_existentes[1:]

    try:
        idx_data = cabecalho.index("Data")
        idx_turma = cabecalho.index("Turma")
    except ValueError as e:
        print(f"Erro ao localizar colunas: {e}")
        return

    index_map = {
        (linha[idx_data], linha[idx_turma]): idx + 3
        for idx, linha in enumerate(dados_existentes)
    }

    colunas_planilha = {col: idx for idx, col in enumerate(cabecalho)}

    for _, row in df_novos.iterrows():
        row = row.fillna('')
        chave = (str(row['Data']), str(row['Turma']))
        valores = row.tolist()

        if chave in index_map:
            linha_idx = index_map[chave]
            cell_range = sheet_destino.range(linha_idx, 1, linha_idx, len(cabecalho))
            for i, cell in enumerate(cell_range):
                if i < len(valores):
                    coluna_nome = cabecalho[i]

                    if coluna_nome == "Data" and isinstance(valores[i], (pd.Timestamp, date)):
                        cell.value = valores[i].strftime("%d/%m/%Y")
                    elif coluna_nome in colunas_numericas:
                        cell.value = int(valores[i]) if pd.notna(valores[i]) else ''
                    else:
                        cell.value = str(valores[i])
                else:
                    cell.value = ''
            sheet_destino.update_cells(cell_range)
            print(f"Atualizado: {chave}")
        else:
            valores_norm = []
            for i, col_name in enumerate(cabecalho):
                v = valores[i] if i < len(valores) else ""
                valores_norm.append(_norm_val(v, col_name, colunas_numericas))

            sheet_destino.append_row(valores_norm, value_input_option='USER_ENTERED')
            print(f"Inserido: {chave}")

        time.sleep(1)

# Atualiza presencial
atualizar_linhas(sheet_presencial, df_presencial)

# Atualiza online
atualizar_linhas(sheet_online, df_online)

# === TIPOS IDEAIS ===
tipos_ideais = {
    1: "DATE", 2: "STRING", 3: "STRING", 4: "STRING",
    5: "NUMBER", 6: "NUMBER", 7: "NUMBER", 8: "STRING",
    9: "NUMBER", 10: "NUMBER", 11: "STRING", 12: "STRING"
}

# === PASSO 1: Detectar linhas erradas ===
result = service_ro.spreadsheets().get(
    spreadsheetId=GOOGLE_SHEET_ID,
    includeGridData=True
).execute()

rows = result["sheets"][0]["data"][0]["rowData"]
linhas_erradas = []

for r_idx, row in enumerate(rows, start=1):
    if r_idx == 1:
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
    """
    Reescreve SEMPRE os valores das colunas de Data e Numéricas nas linhas informadas,
    usando tipos 'USER_ENTERED' que o Sheets reconhece:
      - Data: YYYY-MM-DD (ISO) para garantir tipagem como DATE
      - Numéricos: int/float (sem aspas) para garantir NUMBER

    Depois a formatação visual (dd/MM/yyyy e 0) é aplicada via batchUpdate.
    """
    valores_existentes = sheet_destino.get_all_values()
    if not valores_existentes:
        print("Planilha vazia.")
        return

    cabecalho = valores_existentes[0]
    nome_to_idx = {nome: i for i, nome in enumerate(cabecalho)}

    # Índices das colunas
    idx_data = nome_to_idx.get("Data", None)

    freq_col = "Frequente" if "Frequente" in nome_to_idx else ("Frequentes" if "Frequentes" in nome_to_idx else None)
    colunas_numericas_nomes = ["Vagas", "Integrantes", "Trancados", "Não Frequentes"]
    if freq_col:
        colunas_numericas_nomes.append(freq_col)

    idxs_numericos = [nome_to_idx[c] for c in colunas_numericas_nomes if c in nome_to_idx]

    # helper A1
    def col_idx_to_a1(n):
        s = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    ultima_col_a1 = col_idx_to_a1(len(cabecalho))

    updates = []

    for linha_google in linhas_alvo:
        i = linha_google - 1
        if i <= 0 or i >= len(valores_existentes):
            continue

        linha = list(valores_existentes[i])
        if len(linha) < len(cabecalho):
            linha += [""] * (len(cabecalho) - len(linha))

        # Sempre normaliza: Data -> ISO; Números -> int/float
        if idx_data is not None and idx_data < len(linha):
            raw = linha[idx_data]
            if raw:
                dt = pd.to_datetime(raw, dayfirst=True, errors="coerce")
                # Se não parsear em pt-BR, tenta ISO também
                if pd.isna(dt):
                    dt = pd.to_datetime(raw, errors="coerce")
                if pd.notna(dt):
                    # ISO para Sheets tipar como DATE
                    linha[idx_data] = dt.strftime("%Y-%m-%d")

        for idx_num in idxs_numericos:
            if idx_num < len(linha):
                raw = linha[idx_num]
                if raw == "" or raw is None:
                    continue
                num = pd.to_numeric(str(raw).replace(",", ".").strip(), errors="coerce")
                if pd.notna(num):
                    linha[idx_num] = int(num) if float(num).is_integer() else float(num)
                else:
                    # se tiver lixo (ex.: "-"), zera ou deixa vazio, conforme sua regra
                    linha[idx_num] = ""

        updates.append((linha_google, linha))

    if updates:
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [
                {
                    "range": f"A{lin}:{ultima_col_a1}{lin}",
                    "values": [vals[:len(cabecalho)]],
                }
                for lin, vals in updates
            ],
        }
        service_rw.spreadsheets().values().batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body=body
        ).execute()
        print("✅ Correções reaplicadas (tipagem) nas linhas divergentes.")
    else:
        print("Nenhuma linha para corrigir.")

# === PASSO 3: Forçar formatação das colunas ===
def aplicar_formatacoes(worksheet):
    requests = []

    # Coluna A (Data) -> dd/MM/yyyy
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": worksheet.id,
                "startColumnIndex": 0,
                "endColumnIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    })

    # Colunas numéricas: E (4), F (5), G (6), I (8), J (9)
    for start_idx in [4,5,6,8,9]:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startColumnIndex": start_idx,
                    "endColumnIndex": start_idx+1
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "0"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        })

    service_rw.spreadsheets().batchUpdate(
        spreadsheetId=GOOGLE_SHEET_ID,
        body={"requests": requests}
    ).execute()

    print(f"📅 Formatação aplicada na aba: {worksheet.title}")

# === EXECUTAR ===
if linhas_erradas:
    corrigir_linhas(sheet_presencial, linhas_erradas)
    corrigir_linhas(sheet_online, linhas_erradas)
    aplicar_formatacoes(sheet_presencial)
    aplicar_formatacoes(sheet_online)
    print("✅ Linhas corrigidas e formatação aplicada em ambas as abas!")
else:
    print("✅ Nenhuma linha precisa de correção!")
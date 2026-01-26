#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fix_sedes_google.py

Script para “pente fino” da coluna **Sede** no Google Sheets, com base no FINAL do nome da **Turma**:

- ...72546  -> Aldeota
- ...74070  -> Sul
- ...488365 -> Bezerra

Uso:
  python3 fix_sedes_google.py
  python3 fix_sedes_google.py --spreadsheet-id <ID>
  python3 fix_sedes_google.py --all-sheets
  python3 fix_sedes_google.py --dry-run
  python3 fix_sedes_google.py --sort

Requisitos:
- .env com GOOGLE_CREDENTIALS_JSON (JSON inline ou caminho do arquivo)
- pip install gspread oauth2client google-api-python-client python-dotenv
"""

from __future__ import annotations

import os
import re
import json
import time
import argparse
from typing import Optional, Tuple, List, Dict, Any, Iterator

from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build


# ========= CONFIG =========

load_dotenv()

DEFAULT_SPREADSHEET_ID = os.getenv(
    "GOOGLE_SHEET_ID_FREQ",
    "19_bvzaFfHkHWlRi4dV7hEJ44W2LoJIOSJkWeWW7CQ4A"
)

# Regras de sede por sufixo no FINAL do nome da turma
SEDE_SUFFIX_MAP: Dict[str, str] = {
    "72546": "Aldeota",
    "74070": "Sul",
    "488365": "Bezerra",
}

# Quantas primeiras linhas vamos inspecionar para achar o header
HEADER_SCAN_ROWS = 5

# Tamanho do lote no batchUpdate
BATCH_SIZE = 500


# ========= HELPERS =========

def col_to_a1(col_1based: int) -> str:
    """1 -> A, 2 -> B, ..., 27 -> AA"""
    s = ""
    n = col_1based
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def detectar_sede_por_nome_turma(nome_turma: str, default: str = "") -> str:
    """
    Retorna a sede conforme o sufixo numérico FINAL do texto.
    Se não casar com as regras, retorna default.
    """
    if not isinstance(nome_turma, str):
        return default

    s = nome_turma.strip()

    # pega um bloco numérico no final (ex.: "FS10 72546")
    m = re.search(r"(\d+)\s*$", s)
    if m:
        code = m.group(1)
        if code in SEDE_SUFFIX_MAP:
            return SEDE_SUFFIX_MAP[code]

    # fallback literal endswith
    for code, sede in SEDE_SUFFIX_MAP.items():
        if re.search(rf"{re.escape(code)}\s*$", s):
            return sede

    return default


def build_creds_any(scopes: List[str]):
    """
    Lê GOOGLE_CREDENTIALS_JSON:
      - JSON inline (começa com '{')
      - caminho para o arquivo
    """
    credentials_raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not credentials_raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não definido no .env (JSON inline ou caminho do arquivo).")

    # JSON inline
    if credentials_raw.startswith("{"):
        try:
            cred_dict = json.loads(credentials_raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON parece JSON, mas falhou ao parsear: {e}")
        return ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scopes)

    # Caminho de arquivo
    cred_path = os.path.abspath(credentials_raw)
    if not os.path.exists(cred_path):
        raise FileNotFoundError(f"Caminho de credenciais não existe: {cred_path}")
    return ServiceAccountCredentials.from_json_keyfile_name(cred_path, scopes)


def find_header_row(values: List[List[str]]) -> Optional[int]:
    """
    Retorna o índice (0-based) da linha de header, procurando nas primeiras HEADER_SCAN_ROWS linhas
    uma linha que contenha "Turma" (obrigatório). "Sede" pode existir ou não (criamos se faltar).
    """
    limit = min(len(values), HEADER_SCAN_ROWS)
    for i in range(limit):
        row = [str(c).strip() for c in (values[i] or [])]
        if any(c == "Turma" for c in row):
            return i
    return None


# ✅ CORRIGIDO: como usa yield, isso é Iterator/Generator, não List[List[dict]]
def chunked(lst: List[Dict[str, Any]], n: int) -> Iterator[List[Dict[str, Any]]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ========= CORE =========

def fix_sedes_in_worksheet(
    service_rw,
    spreadsheet_id: str,
    worksheet,
    dry_run: bool = False,
) -> Tuple[int, int, Dict[str, int]]:
    """
    Ajusta a coluna Sede com base na Turma. Retorna:
      (total_linhas_dados, total_alteradas, contagem_por_sede)
    """
    title = worksheet.title
    values = worksheet.get_all_values()

    if not values:
        print(f"• [{title}] Aba vazia — pulando.")
        return 0, 0, {}

    header_idx = find_header_row(values)
    if header_idx is None:
        print(f"• [{title}] Não achei header com coluna 'Turma' nas primeiras {HEADER_SCAN_ROWS} linhas — pulando.")
        return 0, 0, {}

    header = [str(c).strip() for c in values[header_idx]]
    while header and header[-1] == "":
        header.pop()

    if "Turma" not in header:
        print(f"• [{title}] Header encontrado, mas sem 'Turma' — pulando.")
        return 0, 0, {}

    turma_col_1based = header.index("Turma") + 1

    sede_exists = "Sede" in header
    if not sede_exists:
        sede_col_1based = len(header) + 1
        if not dry_run:
            cell = f"{col_to_a1(sede_col_1based)}{header_idx + 1}"
            worksheet.update(cell, "Sede", value_input_option="RAW")
        header.append("Sede")
        print(f"• [{title}] Coluna 'Sede' não existia — criada em {col_to_a1(sede_col_1based)}.")
    else:
        sede_col_1based = header.index("Sede") + 1

    start_row_1based = header_idx + 2
    total_rows = max(0, len(values) - (header_idx + 1))

    updates: List[Dict[str, Any]] = []
    contagem_por_sede: Dict[str, int] = {"Aldeota": 0, "Sul": 0, "Bezerra": 0}

    for offset, row in enumerate(values[header_idx + 1:], start=0):
        row_number = start_row_1based + offset

        if len(row) < max(turma_col_1based, sede_col_1based):
            row = row + [""] * (max(turma_col_1based, sede_col_1based) - len(row))

        turma = str(row[turma_col_1based - 1]).strip()
        sede_atual = str(row[sede_col_1based - 1]).strip()

        if not turma:
            continue

        sede_nova = detectar_sede_por_nome_turma(turma, default="")
        if not sede_nova:
            continue

        if sede_atual != sede_nova:
            rng = f"{title}!{col_to_a1(sede_col_1based)}{row_number}"
            updates.append({"range": rng, "values": [[sede_nova]]})
            contagem_por_sede[sede_nova] = contagem_por_sede.get(sede_nova, 0) + 1

    if not updates:
        print(f"• [{title}] OK — nenhuma divergência encontrada. (linhas analisadas: {total_rows})")
        return total_rows, 0, contagem_por_sede

    print(f"• [{title}] Divergências: {len(updates)} (linhas analisadas: {total_rows})")
    if dry_run:
        print("  (dry-run) Não apliquei alterações.")
        return total_rows, len(updates), contagem_por_sede

    for part in chunked(updates, BATCH_SIZE):
        body = {"valueInputOption": "USER_ENTERED", "data": part}
        service_rw.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        time.sleep(0.4)

    print(f"  ✅ Aplicado: {len(updates)} atualizações na coluna 'Sede'.")
    return total_rows, len(updates), contagem_por_sede


def sort_worksheet_by_data_then_turma(service_rw, spreadsheet_id: str, worksheet, header_row_1based: int):
    values = worksheet.get_all_values()
    if not values or len(values) <= header_row_1based:
        return

    header = [str(c).strip() for c in values[header_row_1based - 1]]
    if "Data" not in header or "Turma" not in header:
        print(f"• [{worksheet.title}] Sem colunas 'Data' e/ou 'Turma' — skip sort.")
        return

    data_col = header.index("Data")   # 0-based
    turma_col = header.index("Turma") # 0-based

    last_row = len(values)
    last_col = max(1, len(header))

    request = {
        "sortRange": {
            "range": {
                "sheetId": worksheet.id,
                "startRowIndex": header_row_1based,
                "endRowIndex": last_row,
                "startColumnIndex": 0,
                "endColumnIndex": last_col,
            },
            "sortSpecs": [
                {"dimensionIndex": data_col, "sortOrder": "ASCENDING"},
                {"dimensionIndex": turma_col, "sortOrder": "ASCENDING"},
            ],
        }
    }

    service_rw.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [request]}
    ).execute()
    print(f"• [{worksheet.title}] ✅ Ordenado por Data -> Turma.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spreadsheet-id", default=DEFAULT_SPREADSHEET_ID, help="ID da planilha do Google Sheets")
    ap.add_argument("--all-sheets", action="store_true", help="Processa todas as abas (por padrão: só 0 e 1)")
    ap.add_argument("--dry-run", action="store_true", help="Não escreve nada; só mostra o que mudaria")
    ap.add_argument("--sort", action="store_true", help="Opcional: organiza ordenando por Data -> Turma")
    args = ap.parse_args()

    scopes_rw = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_rw = build_creds_any(scopes_rw)
    client = gspread.authorize(creds_rw)
    service_rw = build("sheets", "v4", credentials=creds_rw)

    spreadsheet_id = args.spreadsheet_id
    sh = client.open_by_key(spreadsheet_id)

    if args.all_sheets:
        worksheets = sh.worksheets()
    else:
        worksheets = []
        ws0 = sh.get_worksheet(0)
        if ws0:
            worksheets.append(ws0)
        ws1 = sh.get_worksheet(1)
        if ws1:
            worksheets.append(ws1)

    print("=" * 90)
    print("🔎 Pente fino de Sedes (baseado no FINAL da Turma)")
    print(f"Planilha: {spreadsheet_id}")
    print(f"Abas: {[w.title for w in worksheets]}")
    print(f"Dry-run: {'SIM' if args.dry_run else 'NÃO'}")
    print(f"Sort: {'SIM' if args.sort else 'NÃO'}")
    print("=" * 90)

    total_linhas = 0
    total_alteradas = 0
    total_por_sede: Dict[str, int] = {"Aldeota": 0, "Sul": 0, "Bezerra": 0}

    for ws in worksheets:
        values = ws.get_all_values()
        header_idx = find_header_row(values) if values else None
        header_row_1based = (header_idx + 1) if header_idx is not None else 1

        linhas, alteradas, por_sede = fix_sedes_in_worksheet(
            service_rw=service_rw,
            spreadsheet_id=spreadsheet_id,
            worksheet=ws,
            dry_run=args.dry_run,
        )

        total_linhas += linhas
        total_alteradas += alteradas
        for k, v in por_sede.items():
            total_por_sede[k] = total_por_sede.get(k, 0) + v

        if args.sort and not args.dry_run:
            try:
                sort_worksheet_by_data_then_turma(service_rw, spreadsheet_id, ws, header_row_1based)
            except Exception as e:
                print(f"• [{ws.title}] ⚠️ Falha ao ordenar: {e}")

    print("\n" + "=" * 90)
    print("✅ Resumo")
    print(f"Linhas analisadas (aprox): {total_linhas}")
    print(f"Atualizações aplicadas: {total_alteradas}")
    print("Atualizações por sede:")
    for sede in ["Aldeota", "Sul", "Bezerra"]:
        print(f"  - {sede}: {total_por_sede.get(sede, 0)}")
    print("=" * 90)


if __name__ == "__main__":
    main()

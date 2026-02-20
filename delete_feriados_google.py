#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
delete_feriados_google.py

Remove (DELETE) linhas do Google Sheets cuja coluna **Data** cai em feriado
(considerando Brasil + Ceará + feriados municipais de Fortaleza).

Uso:
  python3 delete_feriados_google.py
  python3 delete_feriados_google.py --spreadsheet-id <ID>
  python3 delete_feriados_google.py --all-sheets
  python3 delete_feriados_google.py --dry-run
  python3 delete_feriados_google.py --date-col "Data"
  python3 delete_feriados_google.py --no-fortaleza-municipal

Requisitos:
- .env com GOOGLE_CREDENTIALS_JSON (JSON inline ou caminho do arquivo)
- pip install gspread oauth2client google-api-python-client python-dotenv holidays

Notas:
- Detecta o header procurando "Data" nas primeiras linhas.
- Faz delete via Sheets API (batchUpdate deleteDimension).
- Deleta de baixo pra cima (ou em blocos) pra não bagunçar os índices.
"""

from __future__ import annotations

import os
import json
import time
import argparse
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Iterator, Tuple

from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

import holidays


# ========= CONFIG =========

load_dotenv()

DEFAULT_SPREADSHEET_ID = os.getenv(
    "GOOGLE_SHEET_ID_FREQ",
    "19_bvzaFfHkHWlRi4dV7hEJ44W2LoJIOSJkWeWW7CQ4A"
)

HEADER_SCAN_ROWS = 5
BATCH_SIZE = 200  # requests por batchUpdate (deleteDimension costuma ser mais pesado)


# ========= HELPERS =========

def build_creds_any(scopes: List[str]):
    """
    Lê GOOGLE_CREDENTIALS_JSON:
      - JSON inline (começa com '{')
      - caminho para o arquivo
    """
    credentials_raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not credentials_raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não definido no .env (JSON inline ou caminho do arquivo).")

    if credentials_raw.startswith("{"):
        try:
            cred_dict = json.loads(credentials_raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON parece JSON, mas falhou ao parsear: {e}")
        return ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scopes)

    cred_path = os.path.abspath(credentials_raw)
    if not os.path.exists(cred_path):
        raise FileNotFoundError(f"Caminho de credenciais não existe: {cred_path}")
    return ServiceAccountCredentials.from_json_keyfile_name(cred_path, scopes)


def find_header_row(values: List[List[str]], required_col: str) -> Optional[int]:
    """
    Retorna o índice (0-based) da linha de header, procurando nas primeiras HEADER_SCAN_ROWS linhas
    uma linha que contenha required_col (ex.: "Data").
    """
    limit = min(len(values), HEADER_SCAN_ROWS)
    for i in range(limit):
        row = [str(c).strip() for c in (values[i] or [])]
        if any(c == required_col for c in row):
            return i
    return None


def chunked(lst: List[Dict[str, Any]], n: int) -> Iterator[List[Dict[str, Any]]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def parse_date_br(value: str) -> Optional[date]:
    """
    Converte string da planilha em date().
    Suporta:
      - dd/mm/yyyy
      - dd/mm/yyyy HH:MM
      - dd/mm/yyyy HH:MM:SS
      - yyyy-mm-dd
      - yyyy-mm-dd HH:MM[:SS]
      - dd-mm-yyyy
      - dd/mm/yy
      - yyyy/mm/dd
      - dd.mm.yyyy
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    fmts = [
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%Y/%m/%d",
        "%d.%m.%Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    return None


def easter_date_gregorian(year: int) -> date:
    """
    Computa Domingo de Páscoa (calendário gregoriano) - algoritmo de Meeus/Jones/Butcher.
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def add_carnaval_and_ash_wednesday(hol: holidays.HolidayBase, years: List[int]) -> None:
    """
    Adiciona: Sábado, Domingo, Segunda, Terça de Carnaval + Quarta-feira de Cinzas.
    Base: Páscoa.
    """
    for y in years:
        easter = easter_date_gregorian(y)
        # conforme seu pedido: sábado->terça e quarta
        mapping = {
            50: "Carnaval (Sábado)",
            49: "Carnaval (Domingo)",
            48: "Carnaval (Segunda)",
            47: "Carnaval (Terça)",
            46: "Quarta-feira de Cinzas",
        }
        for delta_days, name in mapping.items():
            hol[easter - timedelta(days=delta_days)] = name


def make_holiday_checker(years: List[int], include_fortaleza_municipal: bool = True):
    """
    Cria feriados BR + CE e (opcional) Fortaleza, e adiciona Carnaval/Cinzas (ponto facultativo/uso interno).
    """
    years = sorted(set([y for y in years if y is not None]))
    if not years:
        years = [datetime.now().year]

    br_ce = holidays.Brazil(subdiv="CE", years=years)

    if include_fortaleza_municipal:
        for y in years:
            br_ce[date(y, 4, 13)] = "Aniversário de Fortaleza"
            br_ce[date(y, 8, 15)] = "Nossa Senhora da Assunção (Fortaleza)"
            br_ce[date(y, 12, 8)] = "Nossa Senhora da Conceição (Fortaleza)"

    add_carnaval_and_ash_wednesday(br_ce, years)

    return br_ce


def group_contiguous(indices_0based: List[int]) -> List[Tuple[int, int]]:
    """
    Agrupa índices de linhas (0-based) contíguos em ranges [start, end_exclusive].
    Ex.: [10,11,12,20,21] -> [(10,13),(20,22)]
    """
    if not indices_0based:
        return []
    idx = sorted(indices_0based)
    ranges: List[Tuple[int, int]] = []
    start = prev = idx[0]
    for x in idx[1:]:
        if x == prev + 1:
            prev = x
            continue
        ranges.append((start, prev + 1))
        start = prev = x
    ranges.append((start, prev + 1))
    return ranges


# ========= CORE =========

def delete_holiday_rows_in_worksheet(
    service_rw,
    spreadsheet_id: str,
    worksheet,
    date_col_name: str = "Data",
    include_fortaleza_municipal: bool = True,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    Deleta linhas cuja Data seja feriado.
    Retorna: (linhas_analisadas, linhas_deletadas)
    """
    title = worksheet.title
    values = worksheet.get_all_values()

    if not values:
        print(f"• [{title}] Aba vazia — pulando.")
        return 0, 0

    header_idx = find_header_row(values, required_col=date_col_name)
    if header_idx is None:
        print(f"• [{title}] Não achei header com coluna '{date_col_name}' nas primeiras {HEADER_SCAN_ROWS} linhas — pulando.")
        return 0, 0

    header = [str(c).strip() for c in values[header_idx] or []]
    if date_col_name not in header:
        print(f"• [{title}] Header encontrado, mas sem '{date_col_name}' — pulando.")
        return 0, 0

    date_col_0 = header.index(date_col_name)

    data_rows = values[header_idx + 1 :]
    if not data_rows:
        print(f"• [{title}] Sem linhas de dados abaixo do header — pulando.")
        return 0, 0

    # Coleta anos presentes para gerar feriados
    years: List[int] = []
    parsed_dates: List[Optional[date]] = []

    for row in data_rows:
        cell = row[date_col_0] if date_col_0 < len(row) else ""
        d = parse_date_br(cell)
        parsed_dates.append(d)
        if d:
            years.append(d.year)

    holiday_set = make_holiday_checker(years, include_fortaleza_municipal=include_fortaleza_municipal)

    # Identifica linhas para deletar
    # Índice real na planilha (0-based) = (header_idx + 1 + offset)
    to_delete_row_indices: List[int] = []
    reasons: Dict[str, int] = {}

    for offset, d in enumerate(parsed_dates):
        if not d:
            continue
        if d in holiday_set:
            sheet_row_index_0 = (header_idx + 1) + offset
            to_delete_row_indices.append(sheet_row_index_0)
            nome = str(holiday_set.get(d, "Feriado")).strip() or "Feriado"
            reasons[nome] = reasons.get(nome, 0) + 1

    linhas_analisadas = len(data_rows)

    if not to_delete_row_indices:
        print(f"• [{title}] OK — nenhum feriado encontrado. (linhas analisadas: {linhas_analisadas})")
        return linhas_analisadas, 0

    # Agrupa em blocos contíguos e deleta de baixo pra cima (pra manter índices)
    ranges = group_contiguous(to_delete_row_indices)
    ranges_desc = sorted(ranges, key=lambda t: t[0], reverse=True)

    total_to_delete = sum((end - start) for start, end in ranges_desc)

    print(f"• [{title}] Feriados detectados: {total_to_delete} linha(s) para deletar (linhas analisadas: {linhas_analisadas})")
    if reasons:
        top = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)
        resumo = ", ".join([f"{k}={v}" for k, v in top[:6]])
        print(f"  Motivos (top): {resumo}")

    if dry_run:
        print("  (dry-run) Não deletei nada.")
        return linhas_analisadas, total_to_delete

    # Monta requests deleteDimension
    requests: List[Dict[str, Any]] = []
    for start, end in ranges_desc:
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": worksheet.id,
                    "dimension": "ROWS",
                    "startIndex": start,
                    "endIndex": end,
                }
            }
        })

    # BatchUpdate em chunks
    deleted = 0
    for part in chunked([{"requests": [r]} for r in requests], BATCH_SIZE):
        # aqui `part` é lista de dicts {"requests":[...]} — vamos consolidar num único body
        merged: List[Dict[str, Any]] = []
        for item in part:
            merged.extend(item["requests"])
        body = {"requests": merged}
        service_rw.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        deleted += sum((r["deleteDimension"]["range"]["endIndex"] - r["deleteDimension"]["range"]["startIndex"]) for r in merged)
        time.sleep(0.5)

    print(f"  ✅ Deletado: {deleted} linha(s).")
    return linhas_analisadas, deleted


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spreadsheet-id", default=DEFAULT_SPREADSHEET_ID, help="ID da planilha do Google Sheets")
    ap.add_argument("--all-sheets", action="store_true", help="Processa todas as abas (por padrão: só 0 e 1)")
    ap.add_argument("--dry-run", action="store_true", help="Não deleta nada; só mostra o que seria deletado")
    ap.add_argument("--date-col", default="Data", help="Nome da coluna de data (default: 'Data')")
    ap.add_argument("--no-fortaleza-municipal", action="store_true", help="Não adiciona feriados municipais de Fortaleza")
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

    include_fortaleza_municipal = (not args.no_fortaleza_municipal)

    print("=" * 90)
    print("🗑️ Delete de linhas em feriados (BR + CE + Fortaleza)")
    print(f"Planilha: {spreadsheet_id}")
    print(f"Abas: {[w.title for w in worksheets]}")
    print(f"Dry-run: {'SIM' if args.dry_run else 'NÃO'}")
    print(f"Coluna de data: {args.date_col}")
    print(f"Fortaleza municipal: {'SIM' if include_fortaleza_municipal else 'NÃO'}")
    print("=" * 90)

    total_analisadas = 0
    total_deletadas = 0

    for ws in worksheets:
        analisadas, deletadas = delete_holiday_rows_in_worksheet(
            service_rw=service_rw,
            spreadsheet_id=spreadsheet_id,
            worksheet=ws,
            date_col_name=args.date_col,
            include_fortaleza_municipal=include_fortaleza_municipal,
            dry_run=args.dry_run,
        )
        total_analisadas += analisadas
        total_deletadas += deletadas

    print("\n" + "=" * 90)
    print("✅ Resumo")
    print(f"Linhas analisadas (aprox): {total_analisadas}")
    print(f"Linhas deletadas: {total_deletadas}")
    print("=" * 90)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import io
import re
import csv
import argparse
import tempfile
from datetime import datetime
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

import gspread
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials


load_dotenv()


# =======================
# CONFIG
# =======================

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public").strip()
DB_TABLE = os.getenv("DB_TABLE", "frequencia").strip()
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer").strip()
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "15"))
AUTO_CREATE_TABLE = str(os.getenv("AUTO_CREATE_TABLE", "1")).strip().lower() in ("1", "true", "yes", "y", "on")

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
DEFAULT_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")


SEDE_SUFFIX_MAP: dict[str, str] = {
    "72546": "Aldeota",
    "74070": "Sul",
    "488365": "Bezerra",
}


# =======================
# LOG
# =======================

def log(msg: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


# =======================
# ARGS
# =======================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Importa planilha do Google Drive/Google Sheets para PostgreSQL com upsert."
    )

    parser.add_argument(
        "--mode",
        required=True,
        choices=["sheets", "drive-file"],
        help="sheets = ler Google Sheets | drive-file = baixar arquivo do Google Drive"
    )

    parser.add_argument(
        "--spreadsheet-id",
        default=None,
        help="ID da planilha do Google Sheets"
    )

    parser.add_argument(
        "--worksheet",
        default=None,
        help="Nome da aba do Google Sheets"
    )

    parser.add_argument(
        "--worksheet-index",
        type=int,
        default=0,
        help="Indice da aba no Google Sheets (default: 0)"
    )

    parser.add_argument(
        "--file-id",
        default=None,
        help="ID do arquivo no Google Drive"
    )

    parser.add_argument(
        "--csv-delimiter",
        default=",",
        help="Delimitador do CSV, se o arquivo for CSV (default: ,)"
    )

    return parser.parse_args()


# =======================
# GOOGLE AUTH
# =======================

def _try_credentials_paths():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        GOOGLE_CREDENTIALS_JSON,
        os.path.join(current_dir, DEFAULT_CREDENTIALS_FILE),
        os.path.join(current_dir, "service-account.json"),
        os.path.expanduser("~/.credentials/credentials.json"),
        os.path.expanduser("~/.credentials/service-account.json"),
    ]

    out = []
    for c in candidates:
        if c and isinstance(c, str):
            p = os.path.abspath(c)
            if os.path.exists(p):
                out.append(p)
    return out


def build_creds(scopes):
    raw = GOOGLE_CREDENTIALS_JSON

    if raw and raw.strip().startswith("{"):
        import json
        cred_dict = json.loads(raw)
        return ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scopes)

    paths = _try_credentials_paths()
    if paths:
        return ServiceAccountCredentials.from_json_keyfile_name(paths[0], scopes)

    raise RuntimeError(
        "Nao encontrei credenciais Google. Defina GOOGLE_CREDENTIALS_JSON no .env "
        "ou garanta que credentials.json exista no diretorio do projeto."
    )


def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = build_creds(scopes)
    return gspread.authorize(creds), creds


def get_drive_service():
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = build_creds(scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


# =======================
# DB
# =======================

def get_db_connection():
    missing = []
    for name, value in [
        ("DB_HOST", DB_HOST),
        ("DB_NAME", DB_NAME),
        ("DB_USER", DB_USER),
        ("DB_PASSWORD", DB_PASSWORD),
    ]:
        if not value:
            missing.append(name)

    if missing:
        raise RuntimeError(f"Variaveis de banco ausentes no .env: {', '.join(missing)}")

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
        connect_timeout=DB_CONNECT_TIMEOUT,
    )


def ensure_postgres_schema_and_table(conn):
    schema_sql = f'CREATE SCHEMA IF NOT EXISTS "{DB_SCHEMA}";'

    table_sql = f'''
    CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}"."{DB_TABLE}" (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        data_aula DATE NOT NULL,
        turma VARCHAR(255) NOT NULL,
        curso VARCHAR(150),
        professor VARCHAR(150),
        vagas INT,
        integrantes INT,
        trancados INT,
        horario VARCHAR(100),
        nao_frequente INT,
        frequente INT,
        dias_semana VARCHAR(100),
        sede VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT uq_{DB_TABLE}_data_turma UNIQUE (data_aula, turma)
    );
    '''

    idx_sql = f'''
    CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_data_aula
        ON "{DB_SCHEMA}"."{DB_TABLE}" (data_aula);
    '''

    with conn.cursor() as cur:
        cur.execute(schema_sql)
        cur.execute(table_sql)
        cur.execute(idx_sql)

    conn.commit()


# =======================
# REGRAS DE NEGOCIO
# =======================

def detectar_sede_por_nome_turma(nome_turma: str, default: str = "") -> str:
    if not isinstance(nome_turma, str):
        return default

    s = nome_turma.strip()

    for code, sede in SEDE_SUFFIX_MAP.items():
        if re.search(rf"(?<!\d){re.escape(code)}(?!\d)", s):
            return sede

    m = re.search(r"(\d+)\s*$", s)
    if m:
        code = m.group(1)
        if code in SEDE_SUFFIX_MAP:
            return SEDE_SUFFIX_MAP[code]

    return default


# =======================
# LEITURA GOOGLE SHEETS
# =======================

def read_google_sheets(spreadsheet_id: str, worksheet: Optional[str], worksheet_index: int) -> pd.DataFrame:
    if not spreadsheet_id:
        raise RuntimeError("--spreadsheet-id e obrigatorio no modo sheets")

    client, _ = get_gspread_client()
    sh = client.open_by_key(spreadsheet_id)

    if worksheet:
        ws = sh.worksheet(worksheet)
    else:
        ws = sh.get_worksheet(worksheet_index)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    header = [str(c).strip() for c in values[0]]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=header)
    return df


# =======================
# LEITURA GOOGLE DRIVE FILE
# =======================

def download_drive_file(file_id: str) -> str:
    if not file_id:
        raise RuntimeError("--file-id e obrigatorio no modo drive-file")

    service = get_drive_service()

    meta = service.files().get(
        fileId=file_id,
        fields="id,name,mimeType"
    ).execute()

    file_name = meta["name"]
    mime_type = meta["mimeType"]

    log(f"Arquivo Drive: {file_name} | mimeType={mime_type}")

    tmp_dir = tempfile.mkdtemp(prefix="drive_import_")

    if mime_type == "application/vnd.google-apps.spreadsheet":
        out_path = os.path.join(tmp_dir, f"{file_name}.xlsx")
        request = service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        ext = os.path.splitext(file_name)[1].lower()
        if not ext:
            ext = ".bin"
        out_path = os.path.join(tmp_dir, f"downloaded{ext}")
        request = service.files().get_media(fileId=file_id)

    fh = io.FileIO(out_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.close()
    return out_path


def read_local_spreadsheet(path: str, delimiter: str = ",") -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo nao encontrado: {path}")

    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(path, sep=delimiter)
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path)

    raise ValueError(f"Formato nao suportado: {ext}")


# =======================
# NORMALIZACAO
# =======================

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "data_aula", "turma", "curso", "professor", "vagas", "integrantes",
            "trancados", "horario", "nao_frequente", "frequente", "dias_semana", "sede"
        ])

    data = df.copy()
    data.columns = [str(c).strip() for c in data.columns]

    rename_map = {
        "Data": "Data",
        "Turma": "Turma",
        "Curso": "Curso",
        "Professor": "Professor",
        "Vagas": "Vagas",
        "Integrantes": "Integrantes",
        "Trancados": "Trancados",
        "Horario": "Horario",
        "NaoFrequente": "NaoFrequente",
        "Não Frequentes": "NaoFrequente",
        "Frequente": "Frequente",
        "Frequentes": "Frequente",
        "DiasSemana": "DiasSemana",
        "Dias da Semana": "DiasSemana",
        "Sede": "Sede",
    }

    for src, dst in rename_map.items():
        if src in data.columns and dst not in data.columns:
            data.rename(columns={src: dst}, inplace=True)

    missing_required = [c for c in ["Data", "Turma"] if c not in data.columns]
    if missing_required:
        raise RuntimeError(
            f"Colunas obrigatorias ausentes: {missing_required}. "
            f"Colunas encontradas: {list(data.columns)}"
        )

    expected = [
        "Data", "Turma", "Curso", "Professor", "Vagas", "Integrantes",
        "Trancados", "Horario", "NaoFrequente", "Frequente", "DiasSemana", "Sede"
    ]

    for col in expected:
        if col not in data.columns:
            data[col] = None

    data = data[expected].copy()

    # remove linhas de cabecalho repetido no meio da planilha
    data = data[
        ~(
            data["Data"].astype(str).str.strip().eq("Data") &
            data["Turma"].astype(str).str.strip().eq("Turma")
        )
    ].copy()

    # Data
    data["Data"] = pd.to_datetime(data["Data"], dayfirst=True, errors="coerce")
    data = data.dropna(subset=["Data"]).copy()

    # textos
    for col in ["Turma", "Curso", "Professor", "Horario", "DiasSemana", "Sede"]:
        data[col] = data[col].apply(
            lambda x: None if pd.isna(x) or str(x).strip() == "" else str(x).strip()
        )

    # detectar sede pela turma
    sede_calc = data["Turma"].apply(lambda x: detectar_sede_por_nome_turma(str(x), default="") if x else "")
    data["Sede"] = sede_calc.where(sede_calc.astype(str).str.strip() != "", data["Sede"])

    # normalizacao numerica robusta
    numeric_cols = ["Vagas", "Integrantes", "Trancados", "NaoFrequente", "Frequente"]
    pg_int_max = 2147483647

    for col in numeric_cols:
        # limpa strings comuns
        data[col] = (
            data[col]
            .astype(str)
            .str.strip()
            .replace({"": None, "None": None, "nan": None, "NaN": None})
        )

        # converte
        data[col] = pd.to_numeric(data[col], errors="coerce")

        # log de valores absurdos antes de limpar
        absurdos = data[data[col].notna() & (data[col].abs() > pg_int_max)]
        if not absurdos.empty:
            print(f"\n[WARN] Valores fora do range INT na coluna {col}:")
            print(absurdos[["Data", "Turma", col]].head(20).to_string(index=False))

        # valores negativos ou absurdos viram nulos
        data.loc[data[col].notna() & (data[col] < 0), col] = None
        data.loc[data[col].notna() & (data[col].abs() > pg_int_max), col] = None

        # inteiros finais
        data[col] = data[col].apply(lambda x: None if pd.isna(x) else int(x))

    # descarta linhas sem turma
    data = data[data["Turma"].notna()].copy()

    # mapeamento final
    data.rename(columns={
        "Data": "data_aula",
        "Turma": "turma",
        "Curso": "curso",
        "Professor": "professor",
        "Vagas": "vagas",
        "Integrantes": "integrantes",
        "Trancados": "trancados",
        "Horario": "horario",
        "NaoFrequente": "nao_frequente",
        "Frequente": "frequente",
        "DiasSemana": "dias_semana",
        "Sede": "sede",
    }, inplace=True)

    data["data_aula"] = data["data_aula"].dt.date

    data = data.drop_duplicates(subset=["data_aula", "turma"], keep="last").reset_index(drop=True)

    return data


# =======================
# UPSERT
# =======================

def upsert_dataframe(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    rows = list(df.itertuples(index=False, name=None))

    sql = f'''
    INSERT INTO "{DB_SCHEMA}"."{DB_TABLE}" (
        data_aula,
        turma,
        curso,
        professor,
        vagas,
        integrantes,
        trancados,
        horario,
        nao_frequente,
        frequente,
        dias_semana,
        sede
    )
    VALUES %s
    ON CONFLICT (data_aula, turma)
    DO UPDATE SET
        curso = EXCLUDED.curso,
        professor = EXCLUDED.professor,
        vagas = EXCLUDED.vagas,
        integrantes = EXCLUDED.integrantes,
        trancados = EXCLUDED.trancados,
        horario = EXCLUDED.horario,
        nao_frequente = EXCLUDED.nao_frequente,
        frequente = EXCLUDED.frequente,
        dias_semana = EXCLUDED.dias_semana,
        sede = EXCLUDED.sede,
        updated_at = CURRENT_TIMESTAMP
    ;
    '''

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        conn.commit()
        return len(rows)

    except Exception as e:
        conn.rollback()

        print("\n[DEBUG] Amostra de linhas que seriam enviadas:")
        for i, row in enumerate(rows[:10], start=1):
            print(f"{i}: {row}")

        raise


# =======================
# MAIN
# =======================

def main():
    args = parse_args()

    log("Iniciando importacao Google Drive/Sheets -> PostgreSQL")

    if args.mode == "sheets":
        df_raw = read_google_sheets(
            spreadsheet_id=args.spreadsheet_id,
            worksheet=args.worksheet,
            worksheet_index=args.worksheet_index,
        )
    else:
        downloaded_path = download_drive_file(args.file_id)
        log(f"Arquivo baixado para: {downloaded_path}")
        df_raw = read_local_spreadsheet(downloaded_path, delimiter=args.csv_delimiter)

    log(f"Linhas lidas: {len(df_raw)}")

    df_norm = normalize_dataframe(df_raw)
    log(f"Linhas validas apos normalizacao: {len(df_norm)}")

    if df_norm.empty:
        log("Nenhum registro valido para importar.")
        return

    conn = None
    try:
        conn = get_db_connection()
        log("Conexao com PostgreSQL estabelecida")

        if AUTO_CREATE_TABLE:
            log(f"Garantindo schema/tabela: {DB_SCHEMA}.{DB_TABLE}")
            ensure_postgres_schema_and_table(conn)

        total = upsert_dataframe(conn, df_norm)
        log(f"Importacao concluida com sucesso. Registros processados: {total}")
        log(f"Destino: {DB_SCHEMA}.{DB_TABLE}")

    except Exception as e:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Falha na importacao: {e}") from e
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
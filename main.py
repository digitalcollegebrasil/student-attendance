#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
main.py

Versão "server-ready" (headless) do script de Frequência de Estudantes, com foco em robustez:
- headless estável + downloads confiáveis (CDP Page.setDownloadBehavior)
- ChromeDriver fixo (/usr/local/bin/chromedriver) via CHROMEDRIVER_PATH
- cliques robustos (intercepted/stale/timeout) + scroll + JS click fallback
- espera de overlays genéricos
- espera de download "novo" (.xls) evitando pegar arquivo antigo
- encerramento seguro do driver (if driver: driver.quit())
- screenshot + page source em caso de erro (debug)
- "PRINTS" (screenshots) em cada passo importante
- logs com contador de passos e timestamp
- pasta por execução dentro de ./debug_sponte_frequencia/<RUN_ID>/

Fluxo:
1) Sponte -> Relatório quantitativo de frequência (Vigente) por data e por sede (Aldeota/Sul/Bezerra)
2) Consolida em Excel local (frequencia_combined_data.xlsx)
3) Gera relatório "100% presença" e envia e-mail (opcional)
4) Sincroniza com Google Sheets (duas abas: Presencial e Online) + correção de tipagem + formatação
5) Pente-fino final: corrige coluna "Sede" no Google Sheets pela Turma (garantia)
6) Pular feriados no Sponte (não coleta em feriado)
7) Pente-fino final de feriados no Google Sheets (deleta linhas cuja Data cai em feriado)

Requisitos:
- google-chrome instalado
- chromedriver em /usr/local/bin/chromedriver (mesma major do Chrome)
- .env com:
    SPONTE_EMAIL, SPONTE_PASSWORD
    GOOGLE_CREDENTIALS_JSON  (JSON inline ou caminho do arquivo)
    EMAIL_USER, EMAIL_PASSWORD (se for enviar e-mail)
  opcionais:
    CHROMEDRIVER_PATH=/usr/local/bin/chromedriver
    SEND_EMAIL=auto|true|false
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587
    EMAIL_FROM=...
    REPORT_DAYS=0 (para filtrar o 100% presença pelos últimos N dias; 0 desliga filtro)
    START_DAYS_AGO=9 (janela padrão de coleta; início = hoje - START_DAYS_AGO)
    END_DAYS_AGO=2   (fim = hoje - END_DAYS_AGO)
    MAX_ATTEMPTS=3

DEBUG:
    STEP_SCREENSHOTS=1 (default: 1)  -> tira prints em cada passo
    STEP_SAVE_HTML=0    (default: 0) -> salva HTML em cada passo (pode pesar)

HEADLESS:
    HEADLESS=1 (default: 1) -> roda headless
    HEADLESS=0 -> roda com UI (útil pra debugar local)

PENTE-FINO SEDES (Sheets):
    FIX_SEDES_GOOGLE=1 (default: 1) -> roda pente-fino após sync
    FIX_SEDES_GOOGLE_SORT=0 (default: 0) -> se 1, ordena por Data -> Turma após correção

FERIADOS:
    SKIP_HOLIDAYS=1 (default: 1) -> pula feriados durante coleta no Sponte
    NO_FORTALEZA_MUNICIPAL=0 (default: 0) -> se 1, não inclui feriados municipais de Fortaleza

PENTE-FINO FERIADOS NO SHEETS:
    PENTE_FINO_FERIADOS_GOOGLE=1 (default: 1) -> remove linhas em feriados no fim
    PENTE_FINO_FERIADOS_DRYRUN=0 (default: 0) -> só simula
    PENTE_FINO_FERIADOS_ALL_SHEETS=0 (default: 0) -> se 1, processa todas as abas
    PENTE_FINO_FERIADOS_DATE_COL=Data (default: Data) -> nome da coluna de data no Sheets
"""

from __future__ import annotations

import os
import re
import argparse
import time
import json
import shutil
import tempfile
from datetime import datetime, timedelta, date
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    WebDriverException,
)

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formataddr
import holidays


# =======================
# CONFIG / ENV
# =======================

load_dotenv()

SPONTE_EMAIL = os.getenv("SPONTE_EMAIL")
SPONTE_PASSWORD = os.getenv("SPONTE_PASSWORD")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

# ChromeDriver path (servidor)
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "/usr/local/bin/chromedriver")

# URLs
url_home = "https://www.sponteeducacional.net.br/home.aspx"
url_didatico = "https://www.sponteeducacional.net.br/SPRel/Didatico/Turmas.aspx"

# Timezone "de negócio"
TZ_NAME = "America/Fortaleza"

# Pastas temp de download/target
current_dir = os.path.dirname(__file__)
DOWNLOAD_TMP = tempfile.mkdtemp(prefix="sponte_dl_")
TARGET_TMP = tempfile.mkdtemp(prefix="sponte_target_")

download_dir = DOWNLOAD_TMP
base_target_dir = TARGET_TMP

os.makedirs(download_dir, exist_ok=True)
os.makedirs(base_target_dir, exist_ok=True)

# Arquivos locais
COMBINED_PATH = os.path.join(current_dir, "frequencia_combined_data.xlsx")
DEBUG_DIR = os.path.join(current_dir, "debug_sponte_frequencia")
os.makedirs(DEBUG_DIR, exist_ok=True)

# ============ PARÂMETROS ============
DESTINATARIOS = [
    "academico.aldeota@digitalcollege.com.br",
    "academico.sul@digitalcollege.com.br",
    "atendimento.bezerra@digitalcollege.com.br",
]
CC: list[str] = []

REPORT_DAYS = int((os.getenv("REPORT_DAYS", "0") or "0"))

START_DAYS_AGO = int((os.getenv("START_DAYS_AGO", "9") or "9"))
END_DAYS_AGO = int((os.getenv("END_DAYS_AGO", "2") or "2"))

MAX_ATTEMPTS = int((os.getenv("MAX_ATTEMPTS", "3") or "3"))

# --- CONTROLE DE ENVIO DE E-MAIL --------------------------------------------
_raw = (os.getenv("SEND_EMAIL", "auto") or "auto").strip().lower()
if _raw in ("1", "true", "yes", "y", "on"):
    SEND_EMAIL = True
elif _raw in ("0", "false", "no", "n", "off"):
    SEND_EMAIL = False
else:
    SEND_EMAIL = bool(EMAIL_USER and EMAIL_PASSWORD)

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
EMAIL_FROM = (os.getenv("EMAIL_FROM", "") or EMAIL_USER or "").strip()

# Google Sheets (frequência)
GOOGLE_SHEET_ID_FREQ = os.getenv(
    "GOOGLE_SHEET_ID_FREQ",
    "19_bvzaFfHkHWlRi4dV7hEJ44W2LoJIOSJkWeWW7CQ4A"
)

# Cabeçalho desejado no Google Sheets (se não existir, cria/força)
SHEET_HEADER_FREQ = [
    "Data",
    "Turma",
    "Curso",
    "Professor",
    "Vagas",
    "Integrantes",
    "Trancados",
    "Horario",
    "NaoFrequente",
    "Frequente",
    "DiasSemana",
    "Sede",
]

# Sedes alvo
HEAD_OFFICES = ["Aldeota", "Sul", "Bezerra"]

# =======================
# REGRAS DE SEDE
# =======================
# Baseado nos códigos de unidade.
SEDE_SUFFIX_MAP: dict[str, str] = {
    "72546": "Aldeota",
    "74070": "Sul",
    "488365": "Bezerra",
}

# "Coisas úteis do antigo": metadados pra debug/validação.
BRANCH_META: dict[str, dict[str, str]] = {
    "Aldeota": {"codigo": "72546"},
    "Sul": {"codigo": "74070"},
    "Bezerra": {"codigo": "488365"},
}

# =======================
# DEBUG / STEPS
# =======================

def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


STEP_SCREENSHOTS = _bool_env("STEP_SCREENSHOTS", False)   # default OFF
STEP_SAVE_HTML = _bool_env("STEP_SAVE_HTML", False)      # default OFF
HEADLESS = _bool_env("HEADLESS", True)                   # default ON

FIX_SEDES_GOOGLE = _bool_env("FIX_SEDES_GOOGLE", True)   # default ON
FIX_SEDES_GOOGLE_SORT = _bool_env("FIX_SEDES_GOOGLE_SORT", False)

# ✅ FERIADOS
SKIP_HOLIDAYS = _bool_env("SKIP_HOLIDAYS", True)
NO_FORTALEZA_MUNICIPAL = _bool_env("NO_FORTALEZA_MUNICIPAL", False)
INCLUDE_FORTALEZA_MUNICIPAL = not NO_FORTALEZA_MUNICIPAL

# ✅ PENTE-FINO FERIADOS NO SHEETS
PENTE_FINO_FERIADOS_GOOGLE = _bool_env("PENTE_FINO_FERIADOS_GOOGLE", True)
PENTE_FINO_FERIADOS_DRYRUN = _bool_env("PENTE_FINO_FERIADOS_DRYRUN", False)
PENTE_FINO_FERIADOS_ALL_SHEETS = _bool_env("PENTE_FINO_FERIADOS_ALL_SHEETS", False)
PENTE_FINO_FERIADOS_DATE_COL = (os.getenv("PENTE_FINO_FERIADOS_DATE_COL", "Data") or "Data").strip()

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
DEBUG_RUN_DIR = os.path.join(DEBUG_DIR, RUN_ID)
os.makedirs(DEBUG_RUN_DIR, exist_ok=True)

_STEP_COUNTER = 0

def _sanitize_label(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_\-\.]+", "", s)
    return s[:80] if s else "step"

def take_snapshot(driver, label: str, save_html: bool = False):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    label = _sanitize_label(label)
    png_path = os.path.join(DEBUG_RUN_DIR, f"{ts}_{label}.png")
    html_path = os.path.join(DEBUG_RUN_DIR, f"{ts}_{label}.html")

    try:
        driver.save_screenshot(png_path)
        print(f"   🖼️ Print salvo: {png_path}")
    except Exception as e:
        print(f"   ⚠️ Falha ao salvar print: {e}")

    if save_html:
        try:
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print(f"   📄 HTML salvo: {html_path}")
        except Exception as e:
            print(f"   ⚠️ Falha ao salvar HTML: {e}")

def step(msg: str, driver=None, snap_label: str | None = None, force_snap: bool = False):
    global _STEP_COUNTER
    _STEP_COUNTER += 1
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{_STEP_COUNTER:03d}] {now} — {msg}")

    if driver and (STEP_SCREENSHOTS or force_snap):
        label = snap_label or msg
        take_snapshot(driver, label=label, save_html=STEP_SAVE_HTML)

def _parse_cli_args():
    """
    Exemplos:
      python3 main.py --inicio 10/01/2026 --fim 15/01/2026 --no-email
      python3 main.py --inicio 2026-01-10 --fim 2026-01-15 --no-email
      python3 main.py --no-email
    """
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--inicio", "--start", dest="inicio", default=None,
                   help="Data inicial (dd/mm/aaaa ou aaaa-mm-dd)")
    p.add_argument("--fim", "--end", dest="fim", default=None,
                   help="Data final (dd/mm/aaaa ou aaaa-mm-dd)")
    p.add_argument("--no-email", dest="no_email", action="store_true",
                   help="Não envia e-mail (força SEND_EMAIL=False)")
    return p.parse_args()

def _parse_date_any(s: str) -> date:
    s = (s or "").strip()
    if not s:
        raise ValueError("data vazia")
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
        return datetime.strptime(s, "%d/%m/%Y").date()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return datetime.strptime(s, "%Y-%m-%d").date()
    raise ValueError(f"Formato inválido: {s} (use dd/mm/aaaa ou aaaa-mm-dd)")


# =======================
# HELPERS (FERIADOS)  ✅ NOVO
# =======================

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
    Cria feriados BR + CE e (opcional) Fortaleza, e adiciona Carnaval/Cinzas.
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


# =======================
# HELPERS (e-mail)
# =======================

def email_configurada() -> bool:
    return bool((EMAIL_FROM or EMAIL_USER) and EMAIL_PASSWORD)

def _resolve_sender() -> str:
    sender = (EMAIL_FROM or EMAIL_USER or "").strip()
    if not sender:
        raise RuntimeError(
            "Remetente ausente. Defina EMAIL_FROM ou EMAIL_USER no .env."
        )
    if "@" not in sender:
        raise RuntimeError(f"Remetente inválido: '{sender}'. Informe um e-mail válido.")
    return sender

def enviar_email(subject: str, html_body: str, attachments: list[str] | None = None):
    attachments = attachments or []

    FROM_ADDR = _resolve_sender()
    LOGIN_USER = (EMAIL_USER or FROM_ADDR)
    if not EMAIL_PASSWORD:
        raise RuntimeError("EMAIL_PASSWORD não definido.")

    msg = MIMEMultipart()
    msg["From"] = formataddr(("Class Panel Bot", FROM_ADDR))
    msg["To"] = ", ".join(DESTINATARIOS)
    if CC:
        msg["Cc"] = ", ".join(CC)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    for path in attachments:
        if not os.path.exists(path):
            print(f"⚠️ Anexo não encontrado: {path}")
            continue
        with open(path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(path))
        part["Content-Disposition"] = f'attachment; filename="{os.path.basename(path)}"'
        msg.attach(part)

    all_rcpts = list(dict.fromkeys((DESTINATARIOS or []) + (CC or [])))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(LOGIN_USER, EMAIL_PASSWORD)
        server.sendmail(FROM_ADDR, all_rcpts, msg.as_string())

    print(f"📧 Email enviado de {FROM_ADDR} para: {', '.join(all_rcpts)}")

def montar_corpo_html_100(df100: pd.DataFrame, hoje_brt: pd.Timestamp, anexo_path: str) -> str:
    if df100.empty:
        return f"""
        <p>Olá,</p>
        <p>Não foram encontradas turmas com <strong>100% de presença</strong> no período considerado.</p>
        <p>Data de geração: <strong>{hoje_brt:%d/%m/%Y %H:%M}</strong></p>
        <p>Anexo: <em>{os.path.basename(anexo_path)}</em></p>
        """

    tbl = df100.copy()
    tbl["Data"] = tbl["Data_dt"].dt.strftime("%d/%m/%Y")
    cols = [c for c in ["Data", "Sede", "Turma", "Curso", "Professor", "Integrantes", "Horario"] if c in tbl.columns]
    tabela_html = tbl[cols].to_html(index=False, border=0, justify="left")

    return f"""
    <p>Olá,</p>
    <p>Segue abaixo o relatório de turmas com <strong>100% de presença</strong> (sem faltas):</p>
    {tabela_html}
    <p>Anexo: <em>{os.path.basename(anexo_path)}</em></p>
    <p>Gerado em: <strong>{hoje_brt:%d/%m/%Y %H:%M}</strong></p>
    """

def enviar_relatorio_turmas_100(df100: pd.DataFrame, anexo_path: str):
    if not SEND_EMAIL or not email_configurada():
        print("E-mail desativado ou credenciais ausentes. Relatório 100% presença foi pulado.")
        return

    hoje_brt = pd.Timestamp.now(tz=TZ_NAME)
    if df100.empty:
        assunto = f"[Relatório] Turmas 100% presença — nenhum registro ({hoje_brt:%d/%m/%Y})"
    else:
        ultimo_dia = df100["Data_dt"].max()
        assunto = f"[Relatório] Turmas 100% presença — até {ultimo_dia:%d/%m/%Y}"

    corpo_html = montar_corpo_html_100(df100, hoje_brt, anexo_path)
    enviar_email(assunto, corpo_html, attachments=[anexo_path])


# =======================
# HELPERS (Sponte/Selenium robustos)
# =======================

def wait_ready(driver, timeout=25):
    WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")

def wait_overlay_gone(driver, timeout=10):
    overlay_selectors = [
        ".blockUI",
        ".blockOverlay",
        ".modal-backdrop",
        ".loading",
        ".spinner",
        "[aria-busy='true']",
    ]
    end = time.time() + timeout
    while time.time() < end:
        try:
            found = False
            for sel in overlay_selectors:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for e in els:
                    try:
                        if e.is_displayed():
                            found = True
                            break
                    except Exception:
                        found = True
                        break
                if found:
                    break
            if not found:
                return
        except Exception:
            pass
        time.sleep(0.3)

def safe_find(driver, by, locator, timeout=20):
    wait = WebDriverWait(driver, timeout)
    return wait.until(EC.presence_of_element_located((by, locator)))

def safe_click(driver, by, locator, timeout=20, attempts=3):
    wait = WebDriverWait(driver, timeout)
    last_exc = None

    for _ in range(attempts):
        try:
            wait_overlay_gone(driver, timeout=8)
            el = wait.until(EC.element_to_be_clickable((by, locator)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center', inline:'center'});", el)
            time.sleep(0.15)
            try:
                el.click()
            except (ElementClickInterceptedException, WebDriverException):
                driver.execute_script("arguments[0].click();", el)
            return
        except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException, WebDriverException) as e:
            last_exc = e
            time.sleep(0.8)

    raise TimeoutException(f"safe_click falhou em {locator}: {last_exc}")

def safe_select_by_visible_text(driver, by, locator, text, timeout=20):
    last_exc = None
    for _ in range(3):
        try:
            el = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, locator)))
            sel = Select(el)
            sel.select_by_visible_text(text)
            return
        except (StaleElementReferenceException, TimeoutException, WebDriverException) as e:
            last_exc = e
            time.sleep(0.8)
    raise TimeoutException(f"safe_select_by_visible_text falhou em {locator}: {last_exc}")

def safe_send_keys(driver, by, locator, text, timeout=20, clear_first=True):
    wait = WebDriverWait(driver, timeout)
    last_exc = None
    for _ in range(3):
        try:
            el = wait.until(EC.presence_of_element_located((by, locator)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.1)
            if clear_first:
                try:
                    el.clear()
                except Exception:
                    pass
            el.send_keys(text)
            return
        except (StaleElementReferenceException, TimeoutException, WebDriverException) as e:
            last_exc = e
            time.sleep(0.8)
    raise TimeoutException(f"safe_send_keys falhou em {locator}: {last_exc}")

def js_set_value_and_events(driver, element, value: str):
    driver.execute_script(
        """
        const el = arguments[0];
        const val = arguments[1];
        el.removeAttribute('value');
        el.value = val;
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        """,
        element,
        value,
    )

def _is_displayed_safe(driver, by, locator) -> bool:
    try:
        el = driver.find_element(by, locator)
        return el.is_displayed()
    except Exception:
        return False

def is_sponte_loading(driver) -> bool:
    """
    True se o loading principal do Sponte estiver visível.
    Cobre:
      - #processing-modal (overlay full screen)
      - container do UpdateProgress (upgProcessando..._upg)
    """
    # 1) Overlay principal
    if _is_displayed_safe(driver, By.ID, "processing-modal"):
        return True

    # 2) Wrapper do UpdateProgress
    if _is_displayed_safe(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_upgProcessando1_upg"):
        return True

    # 3) Fallback por JS (às vezes display none / aria-hidden)
    try:
        return bool(driver.execute_script("""
            const a = document.getElementById('processing-modal');
            const b = document.getElementById('ctl00_ctl00_ContentPlaceHolder1_upgProcessando1_upg');
            function vis(el){
              if(!el) return false;
              const st = window.getComputedStyle(el);
              return st && st.display !== 'none' && st.visibility !== 'hidden' && st.opacity !== '0';
            }
            return vis(a) || vis(b);
        """))
    except Exception:
        return False

def wait_sponte_loading_done(driver, timeout: int = 30):
    """
    Espera o loading principal sumir.
    Não falha se o elemento nem existir.
    """
    def _done(d):
        return not is_sponte_loading(d)

    WebDriverWait(driver, timeout).until(_done)

def wait_for_postback(driver, timeout: int = 25):
    """
    Espera terminar postback/AJAX do Sponte (ASP.NET).
    - Se tiver MS AJAX: Sys.WebForms.PageRequestManager.getInstance().get_isInAsyncPostBack()
    - Senão: jQuery.active
    - + readyState + overlays
    """
    end = time.time() + timeout
    while time.time() < end:
        try:
            wait_overlay_gone(driver, timeout=3)
        except Exception:
            pass

        # 1) ASP.NET AJAX (melhor sinal)
        try:
            in_async = driver.execute_script("""
                try {
                  if (window.Sys && Sys.WebForms && Sys.WebForms.PageRequestManager) {
                    return Sys.WebForms.PageRequestManager.getInstance().get_isInAsyncPostBack();
                  }
                } catch(e) {}
                return null;
            """)
            if in_async is True:
                time.sleep(0.2)
                continue
        except Exception:
            pass

        # 2) jQuery
        try:
            active = driver.execute_script("return (window.jQuery && jQuery.active) ? jQuery.active : 0;")
            if isinstance(active, (int, float)) and active != 0:
                time.sleep(0.2)
                continue
        except Exception:
            pass

        # 3) readyState (fallback)
        try:
            rs = driver.execute_script("return document.readyState")
            if rs == "complete":
                return
        except Exception:
            pass

        time.sleep(0.2)

def ensure_checkbox_state(driver, by, locator, desired: bool = True, timeout: int = 25):
    """
    Garante estado do checkbox e espera o efeito do clique (postback/AJAX).
    Rebusca o elemento para evitar stale.
    """
    last_exc = None
    end = time.time() + timeout

    while time.time() < end:
        try:
            el = safe_find(driver, by, locator, timeout=10)

            try:
                current = bool(el.is_selected())
            except Exception:
                current = str(el.get_attribute("checked") or "").lower() in ("true", "checked", "1")

            if current == desired:
                return

            safe_click(driver, by, locator, timeout=15)
            wait_for_postback(driver, timeout=timeout)
            time.sleep(0.2)
            continue

        except StaleElementReferenceException as e:
            last_exc = e
            time.sleep(0.3)
            continue
        except Exception as e:
            last_exc = e
            time.sleep(0.3)
            continue

    raise TimeoutException(f"ensure_checkbox_state timeout em {locator}: {last_exc}")

def take_debug_snapshot(driver, label: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    png_path = os.path.join(DEBUG_RUN_DIR, f"{ts}_{_sanitize_label(label)}.png")
    html_path = os.path.join(DEBUG_RUN_DIR, f"{ts}_{_sanitize_label(label)}.html")

    try:
        driver.save_screenshot(png_path)
        print(f"🖼️ Screenshot salvo: {png_path}")
    except Exception as e:
        print(f"⚠️ Falha ao salvar screenshot: {e}")

    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print(f"📄 HTML salvo: {html_path}")
    except Exception as e:
        print(f"⚠️ Falha ao salvar HTML: {e}")

def build_driver(download_dir: str, user_data_dir: str):
    chrome_options = webdriver.ChromeOptions()

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    if HEADLESS:
        chrome_options.add_argument("--headless=new")

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--lang=pt-BR")
    if not HEADLESS:
        chrome_options.add_argument("--start-maximized")

    chrome_options.add_argument(f"--user-data-dir={user_data_dir}")

    # service = Service(executable_path=CHROMEDRIVER_PATH)
    # driver = webdriver.Chrome(service=service, options=chrome_options)
    driver = webdriver.Chrome(options=chrome_options)

    driver.set_page_load_timeout(90)
    driver.set_script_timeout(60)

    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": download_dir})
    except Exception as e:
        print(f"⚠️ Não consegui setar download behavior via CDP (pode ainda funcionar): {e}")

    return driver

def wait_for_new_download_xls(download_dir: str, before_files: set[str], timeout=120) -> str:
    end = time.time() + timeout
    last_seen = None

    while time.time() < end:
        crs = [f for f in os.listdir(download_dir) if f.endswith(".crdownload")]
        if crs:
            time.sleep(1)
            continue

        xls = [f for f in os.listdir(download_dir) if f.lower().endswith(".xls") and f not in before_files]
        if xls:
            latest = max(xls, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
            last_seen = os.path.join(download_dir, latest)
            time.sleep(1)
            return last_seen

        time.sleep(1)

    raise TimeoutException(f"Timeout esperando novo download .xls em {download_dir}. Último visto: {last_seen}")

def move_downloaded_file_unique(downloaded_path: str, target_dir: str, current_date: date, head_office: str) -> str:
    filename = f"Relatorio_{current_date.strftime('%d_%m_%Y')}_{head_office}.xls"
    target_path = os.path.join(target_dir, filename)
    shutil.move(downloaded_path, target_path)
    print(f"📥 XLS movido ({head_office} | {current_date:%d/%m/%Y}) -> {target_path}")
    return target_path


# =======================
# LÓGICA DE NEGÓCIO (data/curso/sede)
# =======================

def processar_turma(nome_turma: str | None):
    if not isinstance(nome_turma, str):
        return None
    turmas_ignoradas = ['aulas diversas', 'aulas diversas 2', 'aulas diversas gt']
    nome_norm = nome_turma.lower().strip()
    if any(turma in nome_norm for turma in turmas_ignoradas):
        print(f"Turma ignorada: {nome_turma}")
        return None
    return nome_turma

def detectar_curso(nome_turma: str) -> str:
    if not isinstance(nome_turma, str):
        return ""
    if nome_turma.startswith("CS"):
        return "Cybersecurity"
    elif nome_turma.startswith("FS") and not nome_turma.startswith("FSL"):
        return "Full Stack"
    elif nome_turma.startswith("DA"):
        return "Data Analytics"
    elif nome_turma.startswith("MD"):
        return "Marketing Digital"
    elif nome_turma.startswith("PHP"):
        return "PHP com Laravel"
    elif nome_turma.startswith("UX"):
        return "UX UI"
    elif nome_turma.startswith("PY"):
        return "Python para Dados"
    elif nome_turma.startswith("APM"):
        return "Gerente de Projetos Ágeis"
    elif nome_turma.startswith("FSL"):
        return "Full Stack Live"
    elif nome_turma.startswith("GT"):
        return "Geração Tech"
    return ""

def detectar_sede_por_nome_turma(nome_turma: str, default: str = "") -> str:
    """
    Define Sede procurando os códigos (72546/74070/488365) como "token numérico"
    em QUALQUER POSIÇÃO do nome (com boundary), não só no final.
    """
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

def weekday_pt_for_filter(d: date) -> str:
    mapa = {
        0: "Segunda-Feira",
        1: "Terça-Feira",
        2: "Quarta-Feira",
        3: "Quinta-Feira",
        4: "Sexta-Feira",
        5: "Sábado",
        6: "Domingo",
    }
    return mapa[d.weekday()]


# =======================
# SCRIPT (Sponte -> XLS)
# =======================

def login_sponte(driver) -> Tuple[str, str]:
    step("Abrindo página de login (Sponte Home)", driver, "login_abrindo_home")
    driver.get(url_home)
    wait_ready(driver, timeout=30)
    step("Página de login carregada", driver, "login_home_carregada")

    step("Preenchendo e-mail (txtLogin)", driver, "login_preenchendo_email")
    safe_send_keys(driver, By.ID, "txtLogin", SPONTE_EMAIL, timeout=20)

    step("Preenchendo senha (txtSenha)", driver, "login_preenchendo_senha")
    safe_send_keys(driver, By.ID, "txtSenha", SPONTE_PASSWORD, timeout=20)

    step("Clicando em entrar (btnok)", driver, "login_click_btnok")
    safe_click(driver, By.ID, "btnok", timeout=25)

    step("Aguardando pós-login", driver, "login_pos_login_aguardando")
    time.sleep(2)
    wait_ready(driver, timeout=30)

    nome_empresa = ""
    cod_cliente = ""
    try:
        nome_empresa_el = safe_find(driver, By.ID, "lblNomeEmpresa", timeout=10)
        cod_cliente_el = safe_find(driver, By.ID, "lblCodCliSponte", timeout=10)
        nome_empresa = (nome_empresa_el.text or "").strip()
        cod_cliente_texto = (cod_cliente_el.text or "").strip()
        cod_cliente = "".join(filter(str.isdigit, cod_cliente_texto))
        print(f"🏢 Sede atual (label): {nome_empresa} | Código cliente: {cod_cliente}")
    except Exception:
        pass

    step("Login concluído (página pronta)", driver, "login_concluido")
    return nome_empresa, cod_cliente

def _get_checkbox_label_text(driver, checkbox_id: str) -> str:
    try:
        lab = driver.find_element(By.CSS_SELECTOR, f"label[for='{checkbox_id}']")
        return (lab.text or "").strip()
    except Exception:
        return ""

def selecionar_empresas_por_sede(driver, head_office: str):
    if head_office not in HEAD_OFFICES:
        raise ValueError(f"Sede inválida: {head_office}")

    step(f"Abrindo aba Empresas para selecionar sede: {head_office}", driver, f"empresas_abrindo_{head_office}")
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_liEmpresas", timeout=25)
    wait_for_postback(driver, timeout=25)
    time.sleep(0.5)
    step("Aba Empresas aberta", driver, f"empresas_aberta_{head_office}")

    # pega IDs (strings) — não segura WebElement
    cbs = driver.find_elements(By.CSS_SELECTOR, "input[id^='ctl00_ctl00_ContentPlaceHolder1_cblEmpresas_']")
    cb_ids = [cb.get_attribute("id") for cb in cbs if cb.get_attribute("id")]
    if not cb_ids:
        raise RuntimeError("Não encontrei checkboxes de empresas (cblEmpresas_*).")

    # descobre o alvo
    alvo_id: Optional[str] = None
    codigo = BRANCH_META.get(head_office, {}).get("codigo", "")
    for cid in cb_ids:
        label_txt = _get_checkbox_label_text(driver, cid).lower()
        if codigo and re.search(rf"(?<!\d){re.escape(codigo)}(?!\d)", label_txt):
            alvo_id = cid
            break
        if head_office.lower() in label_txt:
            alvo_id = cid
            break

    if not alvo_id:
        idx_map = {"Aldeota": 0, "Sul": 1, "Bezerra": 2}
        alvo_id = f"ctl00_ctl00_ContentPlaceHolder1_cblEmpresas_{idx_map[head_office]}"

    step(f"Checkbox alvo: {alvo_id} (sede={head_office})", driver, f"empresas_alvo_{head_office}")

    # 1) DESMARCA todos que não são alvo
    for cid in cb_ids:
        if cid == alvo_id:
            continue
        step(f"Desmarcando: {cid}", driver, f"empresas_uncheck_{head_office}")
        ensure_checkbox_state(driver, By.ID, cid, desired=False, timeout=30)

    # 2) MARCA o alvo por último
    step(f"Marcando alvo: {alvo_id}", driver, f"empresas_check_{head_office}")
    ensure_checkbox_state(driver, By.ID, alvo_id, desired=True, timeout=30)

    # validação re-buscando
    time.sleep(0.5)
    selected = []
    for cid in cb_ids:
        try:
            el = driver.find_element(By.ID, cid)
            if el.is_selected():
                selected.append(cid)
        except Exception:
            pass

    if len(selected) != 1:
        print(f"⚠️ Validação: checkboxes selecionados={len(selected)} -> {selected}")
    else:
        print(f"✅ Seleção de empresa OK: {selected[0]}")

    step("Voltando para aba principal (primeira pill)", driver, f"empresas_voltando_principal_{head_office}")
    safe_click(driver, By.CSS_SELECTOR, "ul.nav.nav-pills li:first-child", timeout=25)
    wait_for_postback(driver, timeout=25)
    time.sleep(0.5)
    step("Aba principal selecionada", driver, f"empresas_principal_ok_{head_office}")

def configurar_filtros_frequencia(driver, current_date: date):
    step("Selecionando Situação = Vigente", driver, f"filtros_situacao_vigente_{current_date:%d_%m_%Y}")
    safe_select_by_visible_text(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_cmbSituacaoTurma",
        "Vigente",
        timeout=25,
    )
    time.sleep(0.6)

    dia_pt = weekday_pt_for_filter(current_date)
    step(f"Escolhendo dia da semana: {dia_pt}", driver, f"filtros_dia_{dia_pt}_{current_date:%d_%m_%Y}")
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_divDiaSemana", timeout=25)
    time.sleep(0.5)

    dia_xpath_variants = [f"//*[normalize-space(text())='{dia_pt}']"]
    if dia_pt == "Terça-Feira":
        dia_xpath_variants.append("//*[normalize-space(text())='Terca-Feira']")
    if dia_pt == "Sábado":
        dia_xpath_variants.append("//*[normalize-space(text())='Sabado']")
    if dia_pt == "Domingo":
        dia_xpath_variants.append("//*[normalize-space(text())='Domingo']")

    clicked = False
    for xp in dia_xpath_variants:
        try:
            safe_click(driver, By.XPATH, xp, timeout=10)
            clicked = True
            break
        except Exception:
            pass
    if not clicked:
        raise TimeoutException(f"Não consegui selecionar dia da semana no filtro: {dia_pt}")
    time.sleep(0.6)

    step("Garantindo 'Relatório Quantitativo' marcado", driver, f"filtros_relatorio_quant_{current_date:%d_%m_%Y}")
    ensure_checkbox_state(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkRelatorioQuantitativo",
        desired=True,
        timeout=25,
    )
    time.sleep(0.5)

    step("Garantindo 'Marcar turmas' marcado", driver, f"filtros_marcar_turmas_{current_date:%d_%m_%Y}")
    ensure_checkbox_state(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkMarcarTurmas",
        desired=True,
        timeout=25,
    )
    time.sleep(0.8)

    date_str = current_date.strftime("%d/%m/%Y")

    step(f"Setando data início (JS): {date_str}", driver, f"filtros_data_inicio_{current_date:%d_%m_%Y}")
    start_el = safe_find(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_wcdDataInicioFaltasCons_txtData",
        timeout=25,
    )
    js_set_value_and_events(driver, start_el, date_str)

    step(f"Setando data término (JS): {date_str}", driver, f"filtros_data_termino_{current_date:%d_%m_%Y}")
    end_el = safe_find(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_wcdDataTerminoFaltasCons_txtData",
        timeout=25,
    )
    js_set_value_and_events(driver, end_el, date_str)
    time.sleep(0.6)

    step("Garantindo 'Exportar' marcado", driver, f"filtros_exportar_{current_date:%d_%m_%Y}")
    ensure_checkbox_state(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkExportar", desired=True, timeout=25)
    time.sleep(0.5)

    step("Selecionando tipo exportação: Excel Sem Formatação", driver, f"filtros_tipo_export_{current_date:%d_%m_%Y}")
    safe_click(driver, By.ID, "select2-ctl00_ctl00_ContentPlaceHolder1_cmbTipoExportacao-container", timeout=25)
    time.sleep(0.5)
    safe_click(driver, By.XPATH, "//*[normalize-space(text())='Excel Sem Formatação']", timeout=25)
    time.sleep(0.6)

def baixar_relatorio(driver, current_date: date, head_office: str) -> str:
    step("Capturando lista de arquivos antes do download", driver, f"download_before_{head_office}_{current_date:%d_%m_%Y}")
    before = set(os.listdir(download_dir))

    step("Clicando em 'Gerar' relatório", driver, f"download_click_gerar_{head_office}_{current_date:%d_%m_%Y}")
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_btnGerar_div", timeout=30)
    time.sleep(2)

    step("Aguardando novo download .xls", driver, f"download_wait_{head_office}_{current_date:%d_%m_%Y}")
    downloaded_path = wait_for_new_download_xls(download_dir, before_files=before, timeout=180)
    step(f"Download concluído: {os.path.basename(downloaded_path)}", driver, f"download_done_{head_office}_{current_date:%d_%m_%Y}")

    step("Movendo arquivo baixado para target com nome único", driver, f"download_move_{head_office}_{current_date:%d_%m_%Y}")
    target_path = move_downloaded_file_unique(downloaded_path, base_target_dir, current_date, head_office)
    return target_path

def extrair_df_relatorio(xls_file_path: str, current_date: date, head_office: str) -> pd.DataFrame:
    print(f"📄 Lendo XLS: {xls_file_path}")
    data = pd.read_excel(xls_file_path, skiprows=3)
    data.columns = [c.strip() for c in data.columns]

    ren = {
        "NaoFrequente": "Não Frequentes",
        "Não Frequentes": "Não Frequentes",
        "Frequente": "Frequentes",
        "Frequentes": "Frequentes",
        "DiasSemana": "Dias da Semana",
        "Dias da Semana": "Dias da Semana",
        "Data Início": "DataInicio",
        "DataInicio": "DataInicio",
        "Nome": "Nome",
    }
    for k, v in ren.items():
        if k in data.columns and v not in data.columns:
            data.rename(columns={k: v}, inplace=True)

    if "Nome" in data.columns:
        data["Nome"] = data["Nome"].apply(processar_turma)
        data = data.dropna(subset=["Nome"])

    if data.empty:
        print(f"ℹ️ Sem turmas válidas ({head_office} | {current_date:%d/%m/%Y}).")
        return pd.DataFrame()

    if "DataInicio" in data.columns:
        data["DataInicio"] = pd.to_datetime(data["DataInicio"], dayfirst=True, errors="coerce")
        hoje_brt = pd.Timestamp.now(tz=TZ_NAME).date()
        data = data.dropna(subset=["DataInicio"])
        data = data[data["DataInicio"].dt.date <= hoje_brt].copy()

    if data.empty:
        print(f"ℹ️ Sem registros após filtro DataInicio<=hoje ({head_office} | {current_date:%d/%m/%Y}).")
        return pd.DataFrame()

    data["Data"] = current_date.strftime("%d/%m/%Y")
    data["Curso"] = data["Nome"].apply(detectar_curso) if "Nome" in data.columns else ""

    if "Nome" in data.columns:
        data["Sede_detectada"] = data["Nome"].apply(lambda n: detectar_sede_por_nome_turma(str(n), default=""))
        data["Sede"] = data["Sede_detectada"].where(data["Sede_detectada"].astype(str).str.strip() != "", head_office)

        diverg = data[(data["Sede_detectada"].astype(str).str.strip() != "") & (data["Sede_detectada"] != head_office)]
        if not diverg.empty:
            print(f"⚠️ {len(diverg)} linha(s) no XLS de {head_office} parecem ser de outra sede (filtro Empresas pode estar misto).")
            print(diverg[["Nome", "Sede_detectada"]].head(10))
    else:
        data["Sede"] = head_office

    for c in ["Frequentes", "Não Frequentes", "Integrantes", "Trancados", "Vagas"]:
        if c in data.columns:
            data[c] = pd.to_numeric(data[c], errors="coerce")

    if "Frequentes" in data.columns and "Não Frequentes" in data.columns:
        condicao_remover = (
            ((data["Frequentes"] == 0) & (data["Não Frequentes"] == 0)) |
            ((data["Frequentes"] == 0) & (data["Não Frequentes"].isin([1, 2]))) |
            ((data["Não Frequentes"] == 0) & (data["Frequentes"].isin([1, 2])))
        )
        data = data[~condicao_remover].copy()

    selected_columns = [
        "Data", "Nome", "Curso", "Professor", "Vagas", "Integrantes",
        "Trancados", "Horario", "Não Frequentes", "Frequentes", "Dias da Semana", "Sede"
    ]
    for c in selected_columns:
        if c not in data.columns:
            data[c] = ""

    out = data[selected_columns].copy()
    return out

def run_sponte_frequencia(start_date_range: date | None = None, end_date_range: date | None = None) -> str:
    if not SPONTE_EMAIL or not SPONTE_PASSWORD:
        raise RuntimeError("SPONTE_EMAIL / SPONTE_PASSWORD ausentes no .env.")

    hoje_brt = pd.Timestamp.now(tz=TZ_NAME).date()
    if start_date_range is None:
        start_date_range = hoje_brt - timedelta(days=START_DAYS_AGO)
    if end_date_range is None:
        end_date_range = hoje_brt - timedelta(days=END_DAYS_AGO)

    if start_date_range > end_date_range:
        raise ValueError(
            f"Intervalo inválido: início {start_date_range:%d/%m/%Y} > fim {end_date_range:%d/%m/%Y}"
        )

    # ✅ monta feriados do intervalo (uma vez) para pular no loop
    holiday_set = None
    if SKIP_HOLIDAYS:
        years = list(range(start_date_range.year, end_date_range.year + 1))
        holiday_set = make_holiday_checker(years, include_fortaleza_municipal=INCLUDE_FORTALEZA_MUNICIPAL)

    print("=" * 90)
    print(f"🧭 RUN_ID: {RUN_ID}")
    print(f"🖼️ Prints: {'ON' if STEP_SCREENSHOTS else 'OFF'} | HTML: {'ON' if STEP_SAVE_HTML else 'OFF'}")
    print(f"🧠 Headless: {'ON' if HEADLESS else 'OFF'}")
    print(f"📁 Pasta debug: {DEBUG_RUN_DIR}")
    print(f"🗓️ Janela: {start_date_range:%d/%m/%Y} -> {end_date_range:%d/%m/%Y} (BRT)")
    print(f"🗓️ Pular feriados: {'SIM' if SKIP_HOLIDAYS else 'NÃO'} | Fortaleza municipal: {'SIM' if INCLUDE_FORTALEZA_MUNICIPAL else 'NÃO'}")
    print("=" * 90)

    combined_data: list[pd.DataFrame] = []

    current_date = start_date_range
    while current_date <= end_date_range:
        # domingo
        if current_date.weekday() == 6:
            print(f"⏭️ Pulando Domingo: {current_date:%d/%m/%Y}")
            current_date += timedelta(days=1)
            continue

        # ✅ feriado
        if SKIP_HOLIDAYS and holiday_set is not None and current_date in holiday_set:
            motivo = str(holiday_set.get(current_date, "Feriado")).strip() or "Feriado"
            print(f"⏭️ Pulando FERIADO: {current_date:%d/%m/%Y} — {motivo}")
            current_date += timedelta(days=1)
            continue

        for head_office in HEAD_OFFICES:
            success = False
            for attempt in range(1, MAX_ATTEMPTS + 1):
                user_data_dir = tempfile.mkdtemp(prefix="chrome_profile_")
                driver = None
                label = f"{current_date.strftime('%d_%m_%Y')}_{head_office}_attempt_{attempt}"

                try:
                    step(f"INÍCIO: {current_date:%d/%m/%Y} | {head_office} | tentativa {attempt}/{MAX_ATTEMPTS}")

                    step("Construindo driver Chrome", None)
                    driver = build_driver(download_dir=download_dir, user_data_dir=user_data_dir)
                    step("Driver criado", driver, f"driver_ok_{label}")

                    login_sponte(driver)

                    step("Abrindo URL do relatório didático (Turmas)", driver, f"goto_didatico_{label}")
                    driver.get(url_didatico)
                    wait_ready(driver, timeout=30)
                    step("Página do relatório carregada", driver, f"didatico_ok_{label}")

                    step("Ajustando zoom 100% (se possível)", driver, f"zoom_{label}")
                    try:
                        driver.execute_script("document.body.style.zoom='100%'")
                    except Exception:
                        pass

                    selecionar_empresas_por_sede(driver, head_office=head_office)

                    configurar_filtros_frequencia(driver, current_date=current_date)

                    xls_path = baixar_relatorio(driver, current_date=current_date, head_office=head_office)

                    step("Extraindo dados do XLS para DataFrame", driver, f"extract_df_{label}")
                    df = extrair_df_relatorio(xls_path, current_date=current_date, head_office=head_office)

                    if not df.empty:
                        combined_data.append(df)
                        step(f"✅ Dados adicionados ({head_office} | {current_date:%d/%m/%Y}).", driver, f"df_added_{label}")
                    else:
                        step(f"ℹ️ Sem dados para adicionar ({head_office} | {current_date:%d/%m/%Y}).", driver, f"df_empty_{label}")

                    success = True
                    step(f"FIM OK: {current_date:%d/%m/%Y} | {head_office}", driver, f"fim_ok_{label}")
                    break

                except Exception as e:
                    step(f"❌ Erro ({head_office} | {current_date:%d/%m/%Y}): {e}", driver, f"error_{label}", force_snap=True)
                    if driver:
                        try:
                            take_debug_snapshot(driver, label=f"error_{label}")
                        except Exception:
                            pass

                finally:
                    step("Encerrando driver e limpando perfil temporário", driver, f"cleanup_{label}")
                    if driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                    try:
                        shutil.rmtree(user_data_dir, ignore_errors=True)
                    except Exception:
                        pass

            if not success:
                print(f"⚠️ Falha após {MAX_ATTEMPTS} tentativas: {head_office} | {current_date:%d/%m/%Y}")

        current_date += timedelta(days=1)

    step("Consolidando DataFrames coletados", None)
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=[
            "Data", "Nome", "Curso", "Professor", "Vagas", "Integrantes", "Trancados",
            "Horario", "Não Frequentes", "Frequentes", "Dias da Semana", "Sede"
        ])

    if not final_df.empty and "Nome" in final_df.columns:
        final_df["_sede_code"] = final_df["Nome"].astype(str).apply(lambda n: detectar_sede_por_nome_turma(n, default=""))
        final_df["_has_code"] = (final_df["_sede_code"].astype(str).str.strip() != "").astype(int)
        final_df["Sede"] = final_df["_sede_code"].where(final_df["_has_code"] == 1, final_df.get("Sede", ""))

        final_df = final_df.sort_values(["_has_code"], ascending=False)
        final_df = final_df.drop_duplicates(subset=["Data", "Nome"], keep="first").reset_index(drop=True)
        final_df.drop(columns=["_sede_code", "_has_code"], inplace=True)

    step(f"Salvando Excel consolidado em: {COMBINED_PATH}", None)
    if os.path.exists(COMBINED_PATH):
        os.remove(COMBINED_PATH)
    final_df.to_excel(COMBINED_PATH, index=False)
    print(f"💾 Combined data salvo: {COMBINED_PATH}")

    return COMBINED_PATH


# =======================
# RELATÓRIO 100% PRESENÇA
# =======================

def construir_relatorio_100(df_base: pd.DataFrame) -> pd.DataFrame:
    cols = set(df_base.columns)

    col_freq = "Frequente" if "Frequente" in cols else ("Frequentes" if "Frequentes" in cols else None)
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
        limite = pd.Timestamp.now().normalize() - pd.Timedelta(days=REPORT_DAYS)
        df_base = df_base[df_base["Data_dt"] >= limite]

    mask = (df_base[col_nfreq] == 0) & (df_base[col_freq] == df_base["Integrantes"]) & (df_base["Integrantes"] > 0)

    keep_cols = ["Data_dt", col_turma, "Curso", "Professor", "Integrantes", "Horario", "Sede"]
    keep_cols = [c for c in keep_cols if c in df_base.columns]

    df100 = df_base.loc[mask, keep_cols].copy()

    if col_turma != "Turma" and col_turma in df100.columns:
        df100.rename(columns={col_turma: "Turma"}, inplace=True)

    df100 = df100.sort_values(["Data_dt", "Sede", "Turma"], na_position="last").reset_index(drop=True)
    return df100

def gerar_e_enviar_100_presenca(input_file: str) -> str:
    step("Gerando relatório de 100% presença (lendo Excel consolidado)", None)
    df = pd.read_excel(input_file)
    df.columns = [c.strip() for c in df.columns]

    df.rename(columns={
        "Nome": "Turma",
        "Frequentes": "Frequente",
        "Frequente": "Frequente",
        "NaoFrequente": "Não Frequentes",
        "DiasSemana": "Dias da Semana",
        "Data Início": "DataInicio",
    }, inplace=True)

    if "Turma" in df.columns:
        df = df[~df["Turma"].astype(str).str.startswith("GT")].copy()

    df_100 = construir_relatorio_100(df)

    data_atual_str = pd.Timestamp.now(tz=TZ_NAME).strftime("%d_%m_%Y")
    anexo_path = os.path.join(current_dir, f"turmas_100_presenca_{data_atual_str}.xlsx")

    step(f"Salvando anexo 100% presença em: {anexo_path}", None)
    if not df_100.empty:
        temp = df_100.copy()
        temp["Data"] = temp["Data_dt"].dt.strftime("%d/%m/%Y")
        temp.drop(columns=["Data_dt"], inplace=True)
        temp.to_excel(anexo_path, index=False)
    else:
        pd.DataFrame(columns=["Data", "Turma", "Curso", "Professor", "Integrantes", "Horario", "Sede"]).to_excel(
            anexo_path, index=False
        )

    step("Enviando e-mail do relatório 100% presença (se habilitado)", None)
    enviar_relatorio_turmas_100(df_100, anexo_path)
    step("Relatório 100% presença finalizado", None)
    return anexo_path


# =======================
# GOOGLE SHEETS SYNC (frequência)
# =======================

def _try_paths():
    candidates = [
        os.path.join(current_dir, "credentials.json"),
        os.path.join(current_dir, "service-account.json"),
        os.path.expanduser("~/.credentials/credentials.json"),
        os.path.expanduser("~/.credentials/service-account.json"),
    ]
    seen, uniq = set(), []
    for c in candidates:
        c = os.path.abspath(c)
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return [p for p in uniq if os.path.exists(p)]

def build_creds_any(scopes):
    credentials_raw = os.getenv("GOOGLE_CREDENTIALS_JSON")

    if credentials_raw and credentials_raw.strip().startswith("{"):
        try:
            cred_dict = json.loads(credentials_raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON parece JSON mas falhou ao parsear: {e}")
        return ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scopes)

    if credentials_raw and credentials_raw.strip():
        cred_path = os.path.abspath(credentials_raw.strip())
        if not os.path.exists(cred_path):
            raise FileNotFoundError(f"Caminho de credenciais não existe: {cred_path}")
        return ServiceAccountCredentials.from_json_keyfile_name(cred_path, scopes)

    tried = []
    for path in _try_paths():
        try:
            return ServiceAccountCredentials.from_json_keyfile_name(path, scopes)
        except Exception as e:
            tried.append(f"{path} -> {e}")

    hints = "\n".join(tried) if tried else "Nenhum arquivo candidato encontrado."
    raise RuntimeError(
        "Não encontrei GOOGLE_CREDENTIALS_JSON e o fallback para credentials.json falhou.\n"
        "Defina a env com o CAMINHO do arquivo ou o CONTEÚDO JSON, "
        "ou coloque um credentials.json ao lado do script.\n"
        f"Tentativas:\n{hints}"
    )

def _norm_val(valor, coluna_nome, colunas_numericas):
    if valor is None or (isinstance(valor, float) and pd.isna(valor)) or (isinstance(valor, str) and valor.strip() == ""):
        return ""

    if coluna_nome == "Data":
        if isinstance(valor, (pd.Timestamp, datetime, date)):
            if isinstance(valor, datetime):
                valor = valor.date()
            return valor.strftime("%d/%m/%Y")
        dt = pd.to_datetime(str(valor), dayfirst=True, errors="coerce")
        return dt.strftime("%d/%m/%Y") if pd.notna(dt) else str(valor)

    if coluna_nome in colunas_numericas:
        num = pd.to_numeric(str(valor).replace(",", ".").strip(), errors="coerce")
        if pd.isna(num):
            return ""
        return int(num) if float(num).is_integer() else float(num)

    return str(valor)

def _row_matches_header(row: list[str], expected: list[str]) -> bool:
    if not row:
        return False
    row_norm = [str(c).strip() for c in row]
    exp_norm = [str(c).strip() for c in expected]
    if len(row_norm) < len(exp_norm):
        return False
    return row_norm[:len(exp_norm)] == exp_norm

def ensure_sheet_header(sheet_destino, expected_header: list[str]) -> tuple[list[str], list[list[str]], int]:
    valores_existentes = sheet_destino.get_all_values()

    if not valores_existentes:
        sheet_destino.update("A1", [expected_header], value_input_option="RAW")
        cabecalho = expected_header
        dados_existentes = []
        data_start_row = 2
        print("🧾 Cabeçalho criado (planilha estava vazia).")
        return cabecalho, dados_existentes, data_start_row

    if _row_matches_header(valores_existentes[0], expected_header):
        cabecalho = [c.strip() for c in valores_existentes[0][:len(expected_header)]]
        dados_existentes = valores_existentes[1:]
        data_start_row = 2
        return cabecalho, dados_existentes, data_start_row

    if len(valores_existentes) >= 2 and _row_matches_header(valores_existentes[1], expected_header):
        cabecalho = [c.strip() for c in valores_existentes[1][:len(expected_header)]]
        dados_existentes = valores_existentes[2:]
        data_start_row = 3
        return cabecalho, dados_existentes, data_start_row

    sheet_destino.update("A1", [expected_header], value_input_option="RAW")
    valores_existentes = sheet_destino.get_all_values()
    cabecalho = expected_header
    dados_existentes = valores_existentes[1:] if len(valores_existentes) > 1 else []
    data_start_row = 2
    print("🧾 Cabeçalho forçado na linha 1 (não existia no formato esperado).")
    return cabecalho, dados_existentes, data_start_row

def atualizar_linhas(sheet_destino, df_novos: pd.DataFrame, colunas_numericas: list[str]):
    cabecalho, dados_existentes, data_start_row = ensure_sheet_header(sheet_destino, SHEET_HEADER_FREQ)

    try:
        idx_data = cabecalho.index("Data")
        idx_turma = cabecalho.index("Turma")
    except ValueError as e:
        print(f"Erro ao localizar colunas no cabeçalho: {e}")
        return

    index_map = {}
    for i, linha in enumerate(dados_existentes):
        if len(linha) < len(cabecalho):
            linha = linha + [""] * (len(cabecalho) - len(linha))
        chave = (linha[idx_data], linha[idx_turma])
        if chave != ("", ""):
            index_map[chave] = i + data_start_row

    for _, row in df_novos.iterrows():
        row = row.fillna("")
        chave = (str(row.get("Data", "")), str(row.get("Turma", "")))

        valores_alinhados = []
        for col_name in cabecalho:
            v = row.get(col_name, "")
            valores_alinhados.append(v)

        if chave in index_map:
            linha_idx = index_map[chave]
            cell_range = sheet_destino.range(linha_idx, 1, linha_idx, len(cabecalho))
            for i_cell, cell in enumerate(cell_range):
                col_name = cabecalho[i_cell]
                v = valores_alinhados[i_cell] if i_cell < len(valores_alinhados) else ""

                if col_name == "Data":
                    dt = pd.to_datetime(str(v), dayfirst=True, errors="coerce")
                    cell.value = dt.strftime("%d/%m/%Y") if pd.notna(dt) else str(v)
                elif col_name in colunas_numericas:
                    num = pd.to_numeric(str(v).replace(",", ".").strip(), errors="coerce")
                    cell.value = "" if pd.isna(num) else (int(num) if float(num).is_integer() else float(num))
                else:
                    cell.value = str(v)

            sheet_destino.update_cells(cell_range)
            print(f"Atualizado: {chave}")
        else:
            valores_norm = []
            for i_col, col_name in enumerate(cabecalho):
                v = valores_alinhados[i_col] if i_col < len(valores_alinhados) else ""
                valores_norm.append(_norm_val(v, col_name, colunas_numericas))
            sheet_destino.append_row(valores_norm, value_input_option="USER_ENTERED")
            print(f"Inserido: {chave}")

        time.sleep(0.6)

def detectar_linhas_divergentes_por_sheet(service_ro, spreadsheet_id: str, sheet_index: int, tipos_ideais: dict[int, str]) -> list[int]:
    result = service_ro.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        includeGridData=True
    ).execute()

    sheets = result.get("sheets", [])
    if sheet_index >= len(sheets):
        return []

    rows = (sheets[sheet_index].get("data", [{}])[0] or {}).get("rowData", []) or []
    linhas_erradas: list[int] = []

    for r_idx, row in enumerate(rows, start=1):
        if r_idx == 1:
            continue
        if "values" not in row:
            continue

        erros = []
        for c_idx, cell in enumerate(row["values"], start=1):
            user_value = cell.get("userEnteredValue", {}) or {}
            effective_value = cell.get("effectiveValue", {}) or {}
            number_format = (cell.get("userEnteredFormat", {}) or {}).get("numberFormat", {}) or {}

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
            print(f"⚠️ Sheet[{sheet_index}] Linha {r_idx} divergente → {erros}")
            linhas_erradas.append(r_idx)

    return linhas_erradas

def corrigir_linhas_tipagem(service_rw, spreadsheet_id: str, worksheet, linhas_alvo: list[int], colunas_numericas_nomes: list[str]):
    valores_existentes = worksheet.get_all_values()
    if not valores_existentes:
        return

    cabecalho = valores_existentes[0]
    nome_to_idx = {nome: i for i, nome in enumerate(cabecalho)}

    idx_data = nome_to_idx.get("Data", None)
    idxs_numericos = [nome_to_idx[c] for c in colunas_numericas_nomes if c in nome_to_idx]

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

        if idx_data is not None and idx_data < len(linha):
            raw = linha[idx_data]
            if raw:
                dt = pd.to_datetime(raw, dayfirst=True, errors="coerce")
                if pd.isna(dt):
                    dt = pd.to_datetime(raw, errors="coerce")
                if pd.notna(dt):
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
                    linha[idx_num] = ""

        updates.append((linha_google, linha[:len(cabecalho)]))

    if not updates:
        return

    body = {
        "valueInputOption": "USER_ENTERED",
        "data": [
            {"range": f"A{lin}:{ultima_col_a1}{lin}", "values": [vals]}
            for lin, vals in updates
        ],
    }
    service_rw.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()
    print(f"✅ Tipagem reaplicada em {len(updates)} linha(s) na aba: {worksheet.title}")

def aplicar_formatacoes_attendance(service_rw, spreadsheet_id: str, worksheet):
    requests = []

    requests.append({
        "repeatCell": {
            "range": {"sheetId": worksheet.id, "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
            "fields": "userEnteredFormat.numberFormat"
        }
    })

    for start_idx in [4, 5, 6, 8, 9]:
        requests.append({
            "repeatCell": {
                "range": {"sheetId": worksheet.id, "startColumnIndex": start_idx, "endColumnIndex": start_idx + 1},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        })

    service_rw.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()
    print(f"📅 Formatação aplicada na aba: {worksheet.title}")

def google_sheets_sync_frequencia(input_file: str):
    step("Iniciando sync com Google Sheets (lendo Excel consolidado)", None)
    df = pd.read_excel(input_file)
    df.columns = [c.strip() for c in df.columns]

    df.rename(columns={
        "Nome": "Turma",
        "Frequentes": "Frequente",
        "Frequente": "Frequente",
        "NaoFrequente": "Não Frequentes",
        "DiasSemana": "Dias da Semana",
        "Data Início": "DataInicio",
    }, inplace=True)

    if "Turma" not in df.columns or "Data" not in df.columns:
        raise RuntimeError("Colunas 'Turma' e 'Data' são necessárias no arquivo consolidado.")

    df = df[~df["Turma"].astype(str).str.startswith("GT")].copy()

    if "Sede" not in df.columns:
        df["Sede"] = ""
    sede_calc = df["Turma"].astype(str).apply(lambda n: detectar_sede_por_nome_turma(n, default=""))
    df["Sede"] = sede_calc.where(sede_calc.astype(str).str.strip() != "", df["Sede"])

    df.rename(columns={
        "Não Frequentes": "NaoFrequente",
        "Dias da Semana": "DiasSemana",
    }, inplace=True)

    for c in SHEET_HEADER_FREQ:
        if c not in df.columns:
            df[c] = ""
    df = df[SHEET_HEADER_FREQ].copy()

    colunas_numericas = ["Vagas", "Integrantes", "Trancados", "Frequente", "NaoFrequente"]
    for coluna in colunas_numericas:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    df_online = df[df["Turma"].astype(str).str.len().ge(3) & (df["Turma"].astype(str).str[2].str.upper() == "L")].copy()
    df_presencial = df[~(df["Turma"].astype(str).str.len().ge(3) & (df["Turma"].astype(str).str[2].str.upper() == "L"))].copy()

    step("Autenticando Google Sheets (RW e RO)", None)
    scope_rw = ["https://www.googleapis.com/auth/spreadsheets"]
    scope_ro = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds_rw = build_creds_any(scope_rw)
    creds_ro = build_creds_any(scope_ro)

    client = gspread.authorize(creds_rw)
    service_ro = build("sheets", "v4", credentials=creds_ro)
    service_rw = build("sheets", "v4", credentials=creds_rw)

    sheet = client.open_by_key(GOOGLE_SHEET_ID_FREQ)
    sheet_presencial = sheet.get_worksheet(0)
    sheet_online = sheet.get_worksheet(1)

    step("Atualizando linhas (Aba Presencial)", None)
    atualizar_linhas(sheet_presencial, df_presencial, colunas_numericas=colunas_numericas)

    step("Atualizando linhas (Aba Online)", None)
    atualizar_linhas(sheet_online, df_online, colunas_numericas=colunas_numericas)

    tipos_ideais = {
        1: "DATE", 2: "STRING", 3: "STRING", 4: "STRING",
        5: "NUMBER", 6: "NUMBER", 7: "NUMBER", 8: "STRING",
        9: "NUMBER", 10: "NUMBER", 11: "STRING", 12: "STRING"
    }

    step("Detectando divergências de tipagem (Presencial)", None)
    linhas_erradas_presencial = detectar_linhas_divergentes_por_sheet(
        service_ro, GOOGLE_SHEET_ID_FREQ, sheet_index=0, tipos_ideais=tipos_ideais
    )

    step("Detectando divergências de tipagem (Online)", None)
    linhas_erradas_online = detectar_linhas_divergentes_por_sheet(
        service_ro, GOOGLE_SHEET_ID_FREQ, sheet_index=1, tipos_ideais=tipos_ideais
    )

    if linhas_erradas_presencial:
        step(f"Corrigindo tipagem em {len(linhas_erradas_presencial)} linha(s) (Presencial)", None)
        corrigir_linhas_tipagem(
            service_rw,
            GOOGLE_SHEET_ID_FREQ,
            sheet_presencial,
            linhas_erradas_presencial,
            colunas_numericas_nomes=["Vagas", "Integrantes", "Trancados", "NaoFrequente", "Frequente"],
        )
        aplicar_formatacoes_attendance(service_rw, GOOGLE_SHEET_ID_FREQ, sheet_presencial)

    if linhas_erradas_online:
        step(f"Corrigindo tipagem em {len(linhas_erradas_online)} linha(s) (Online)", None)
        corrigir_linhas_tipagem(
            service_rw,
            GOOGLE_SHEET_ID_FREQ,
            sheet_online,
            linhas_erradas_online,
            colunas_numericas_nomes=["Vagas", "Integrantes", "Trancados", "NaoFrequente", "Frequente"],
        )
        aplicar_formatacoes_attendance(service_rw, GOOGLE_SHEET_ID_FREQ, sheet_online)

    if not linhas_erradas_presencial and not linhas_erradas_online:
        print("✅ Nenhuma linha precisa de correção de tipagem!")

    step("✅ Sincronização Google Sheets concluída", None)

    if FIX_SEDES_GOOGLE:
        try:
            step("🔧 Rodando pente-fino final de Sedes no Google Sheets", None)
            fix_sedes_google_sheet(
                spreadsheet_id=GOOGLE_SHEET_ID_FREQ,
                all_sheets=False,
                dry_run=False,
                sort_after=FIX_SEDES_GOOGLE_SORT,
                creds_rw=creds_rw,
            )
        except Exception as e:
            print(f"⚠️ Falha no pente-fino de sedes (Sheets): {e}")


# =======================
# PENTE FINO: FIX SEDES NO GOOGLE SHEETS
# =======================

HEADER_SCAN_ROWS = 5
BATCH_SIZE = 500

def col_to_a1(col_1based: int) -> str:
    s = ""
    n = col_1based
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def find_header_row(values: List[List[str]]) -> Optional[int]:
    limit = min(len(values), HEADER_SCAN_ROWS)
    for i in range(limit):
        row = [str(c).strip() for c in (values[i] or [])]
        if any(c == "Turma" for c in row):
            return i
    return None

def chunked(lst: List[Dict[str, Any]], n: int) -> Iterator[List[Dict[str, Any]]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fix_sedes_in_worksheet(
    service_rw,
    spreadsheet_id: str,
    worksheet,
    dry_run: bool = False,
) -> Tuple[int, int, Dict[str, int]]:
    title = worksheet.title
    values = worksheet.get_all_values()

    if not values:
        print(f"• [{title}] Aba vazia — pulando.")
        return 0, 0, {}

    header_idx = find_header_row(values)
    if header_idx is None:
        print(f"• [{title}] Não achei header com 'Turma' nas primeiras {HEADER_SCAN_ROWS} linhas — pulando.")
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
        time.sleep(0.3)

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

    data_col = header.index("Data")
    turma_col = header.index("Turma")

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

def fix_sedes_google_sheet(
    spreadsheet_id: str,
    all_sheets: bool = False,
    dry_run: bool = False,
    sort_after: bool = False,
    creds_rw=None,
):
    if creds_rw is None:
        scopes_rw = ["https://www.googleapis.com/auth/spreadsheets"]
        creds_rw = build_creds_any(scopes_rw)

    client = gspread.authorize(creds_rw)
    service_rw = build("sheets", "v4", credentials=creds_rw)

    sh = client.open_by_key(spreadsheet_id)

    if all_sheets:
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
    print("🔎 Pente fino de Sedes (códigos 72546/74070/488365 na Turma)")
    print(f"Planilha: {spreadsheet_id}")
    print(f"Abas: {[w.title for w in worksheets]}")
    print(f"Dry-run: {'SIM' if dry_run else 'NÃO'} | Sort: {'SIM' if sort_after else 'NÃO'}")
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
            dry_run=dry_run,
        )

        total_linhas += linhas
        total_alteradas += alteradas
        for k, v in por_sede.items():
            total_por_sede[k] = total_por_sede.get(k, 0) + v

        if sort_after and not dry_run:
            try:
                sort_worksheet_by_data_then_turma(service_rw, spreadsheet_id, ws, header_row_1based)
            except Exception as e:
                print(f"• [{ws.title}] ⚠️ Falha ao ordenar: {e}")

    print("\n" + "=" * 90)
    print("✅ Resumo pente-fino sedes")
    print(f"Linhas analisadas (aprox): {total_linhas}")
    print(f"Atualizações aplicadas: {total_alteradas}")
    print("Atualizações por sede:")
    for sede in ["Aldeota", "Sul", "Bezerra"]:
        print(f"  - {sede}: {total_por_sede.get(sede, 0)}")
    print("=" * 90)


# =======================
# ✅ PENTE-FINO FERIADOS NO GOOGLE SHEETS
# =======================

HOL_HEADER_SCAN_ROWS = 5
HOL_BATCH_SIZE = 200

def hol_find_header_row(values: List[List[str]], required_col: str) -> Optional[int]:
    limit = min(len(values), HOL_HEADER_SCAN_ROWS)
    for i in range(limit):
        row = [str(c).strip() for c in (values[i] or [])]
        if any(c == required_col for c in row):
            return i
    return None

def hol_parse_date_br(value: str) -> Optional[date]:
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

def hol_group_contiguous(indices_0based: List[int]) -> List[Tuple[int, int]]:
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

def hol_chunked(lst: List[Dict[str, Any]], n: int) -> Iterator[List[Dict[str, Any]]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def delete_holiday_rows_in_worksheet(
    service_rw,
    spreadsheet_id: str,
    worksheet,
    date_col_name: str = "Data",
    include_fortaleza_municipal: bool = True,
    dry_run: bool = False,
) -> Tuple[int, int]:
    title = worksheet.title
    values = worksheet.get_all_values()

    if not values:
        print(f"• [{title}] Aba vazia — pulando.")
        return 0, 0

    header_idx = hol_find_header_row(values, required_col=date_col_name)
    if header_idx is None:
        print(f"• [{title}] Não achei header com coluna '{date_col_name}' nas primeiras {HOL_HEADER_SCAN_ROWS} linhas — pulando.")
        return 0, 0

    header = [str(c).strip() for c in values[header_idx] or []]
    if date_col_name not in header:
        print(f"• [{title}] Header encontrado, mas sem '{date_col_name}' — pulando.")
        return 0, 0

    date_col_0 = header.index(date_col_name)

    data_rows = values[header_idx + 1:]
    if not data_rows:
        print(f"• [{title}] Sem linhas de dados abaixo do header — pulando.")
        return 0, 0

    years: List[int] = []
    parsed_dates: List[Optional[date]] = []

    for row in data_rows:
        cell = row[date_col_0] if date_col_0 < len(row) else ""
        d = hol_parse_date_br(cell)
        parsed_dates.append(d)
        if d:
            years.append(d.year)

    holiday_set = make_holiday_checker(years, include_fortaleza_municipal=include_fortaleza_municipal)

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

    ranges = hol_group_contiguous(to_delete_row_indices)
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

    deleted = 0
    for part in hol_chunked([{"requests": [r]} for r in requests], HOL_BATCH_SIZE):
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

def pente_fino_feriados_google_sheet(
    spreadsheet_id: str,
    all_sheets: bool = False,
    dry_run: bool = False,
    date_col: str = "Data",
    include_fortaleza_municipal: bool = True,
    creds_rw=None,
) -> Tuple[int, int]:
    if creds_rw is None:
        scopes_rw = ["https://www.googleapis.com/auth/spreadsheets"]
        creds_rw = build_creds_any(scopes_rw)

    client = gspread.authorize(creds_rw)
    service_rw = build("sheets", "v4", credentials=creds_rw)

    sh = client.open_by_key(spreadsheet_id)

    if all_sheets:
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
    print("🗑️ Pente-fino de feriados (BR + CE + Fortaleza + Carnaval/Cinzas)")
    print(f"Planilha: {spreadsheet_id}")
    print(f"Abas: {[w.title for w in worksheets]}")
    print(f"Dry-run: {'SIM' if dry_run else 'NÃO'}")
    print(f"Coluna de data: {date_col}")
    print(f"Fortaleza municipal: {'SIM' if include_fortaleza_municipal else 'NÃO'}")
    print("=" * 90)

    total_analisadas = 0
    total_deletadas = 0

    for ws in worksheets:
        analisadas, deletadas = delete_holiday_rows_in_worksheet(
            service_rw=service_rw,
            spreadsheet_id=spreadsheet_id,
            worksheet=ws,
            date_col_name=date_col,
            include_fortaleza_municipal=include_fortaleza_municipal,
            dry_run=dry_run,
        )
        total_analisadas += analisadas
        total_deletadas += deletadas

    print("\n" + "=" * 90)
    print("✅ Resumo pente-fino feriados")
    print(f"Linhas analisadas (aprox): {total_analisadas}")
    print(f"Linhas deletadas: {total_deletadas}")
    print("=" * 90)

    return total_analisadas, total_deletadas


# =======================
# MAIN
# =======================

def main():
    global SEND_EMAIL
    args = _parse_cli_args()

    start_dt = _parse_date_any(args.inicio) if args.inicio else None
    end_dt = _parse_date_any(args.fim) if args.fim else None

    if args.no_email or args.inicio or args.fim:
        SEND_EMAIL = False

    step("START do pipeline", None)

    output_path = run_sponte_frequencia(start_date_range=start_dt, end_date_range=end_dt)
    step("Coleta Sponte concluída (Excel consolidado pronto)", None)

    if SEND_EMAIL:
        gerar_e_enviar_100_presenca(output_path)
        step("Relatório 100% presença concluído", None)
    else:
        step("Modo NO-EMAIL ativo: pulando relatório 100% presença", None)

    google_sheets_sync_frequencia(output_path)
    step("Sync Google Sheets concluído", None)

    if PENTE_FINO_FERIADOS_GOOGLE:
        try:
            step("🗑️ Rodando pente-fino final de feriados no Google Sheets", None)
            pente_fino_feriados_google_sheet(
                spreadsheet_id=GOOGLE_SHEET_ID_FREQ,
                all_sheets=PENTE_FINO_FERIADOS_ALL_SHEETS,
                dry_run=PENTE_FINO_FERIADOS_DRYRUN,
                date_col=PENTE_FINO_FERIADOS_DATE_COL,
                include_fortaleza_municipal=INCLUDE_FORTALEZA_MUNICIPAL,
                creds_rw=None,
            )

            # opcional: roda de novo o pente-fino de sedes depois de deletar linhas
            if FIX_SEDES_GOOGLE and not PENTE_FINO_FERIADOS_DRYRUN:
                step("🔧 Reaplicando pente-fino de Sedes após remover feriados", None)
                fix_sedes_google_sheet(
                    spreadsheet_id=GOOGLE_SHEET_ID_FREQ,
                    all_sheets=False,
                    dry_run=False,
                    sort_after=FIX_SEDES_GOOGLE_SORT,
                    creds_rw=None,
                )

        except Exception as e:
            print(f"⚠️ Falha no pente-fino de feriados (Sheets): {e}")

    step("FIM do pipeline (tudo concluído)", None)
    print(f"📌 Debug/prints desta execução: {DEBUG_RUN_DIR}")

if __name__ == "__main__":
    main()
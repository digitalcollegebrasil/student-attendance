#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
main.py

Versão "server-ready" (headless) do script de Frequência de Estudantes,
adaptada para PostgreSQL com UPSERT.

Fluxo:
1) Sponte -> Relatório quantitativo de frequência (Vigente) por data e por sede
2) Consolida em Excel local (frequencia_combined_data.xlsx)
3) Gera relatório "100% presença" e envia e-mail (opcional)
4) Sincroniza com PostgreSQL usando UPSERT
5) Pula feriados no Sponte

Requisitos:
- google-chrome instalado
- chromedriver compatível com o Chrome
- PostgreSQL acessível
- .env com:
    SPONTE_EMAIL, SPONTE_PASSWORD
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    EMAIL_USER, EMAIL_PASSWORD (se for enviar e-mail)

Opcionais:
    CHROMEDRIVER_PATH=/usr/local/bin/chromedriver
    SEND_EMAIL=auto|true|false
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587
    EMAIL_FROM=...
    REPORT_DAYS=0
    START_DAYS_AGO=9
    END_DAYS_AGO=2
    MAX_ATTEMPTS=3
    DB_SCHEMA=public
    DB_TABLE=frequencia
    DB_SSLMODE=prefer
    DB_CONNECT_TIMEOUT=15
    AUTO_CREATE_TABLE=1
    STEP_SCREENSHOTS=0
    STEP_SAVE_HTML=0
    HEADLESS=1
    SKIP_HOLIDAYS=1
    NO_FORTALEZA_MUNICIPAL=0
"""

from __future__ import annotations

import os
import re
import argparse
import time
import shutil
import tempfile
import smtplib
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import holidays
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    WebDriverException,
)

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formataddr


# =======================
# CONFIG / ENV
# =======================

load_dotenv()

SPONTE_EMAIL = os.getenv("SPONTE_EMAIL")
SPONTE_PASSWORD = os.getenv("SPONTE_PASSWORD")

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "/usr/local/bin/chromedriver")

url_home = "https://www.sponteeducacional.net.br/home.aspx"
url_didatico = "https://www.sponteeducacional.net.br/SPRel/Didatico/Turmas.aspx"

TZ_NAME = "America/Fortaleza"

current_dir = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_TMP = tempfile.mkdtemp(prefix="sponte_dl_")
TARGET_TMP = tempfile.mkdtemp(prefix="sponte_target_")

download_dir = DOWNLOAD_TMP
base_target_dir = TARGET_TMP

os.makedirs(download_dir, exist_ok=True)
os.makedirs(base_target_dir, exist_ok=True)

COMBINED_PATH = os.path.join(current_dir, "frequencia_combined_data.xlsx")
DEBUG_DIR = os.path.join(current_dir, "debug_sponte_frequencia")
os.makedirs(DEBUG_DIR, exist_ok=True)

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

HEAD_OFFICES = ["Aldeota", "Sul", "Bezerra"]

SEDE_SUFFIX_MAP: dict[str, str] = {
    "72546": "Aldeota",
    "74070": "Sul",
    "488365": "Bezerra",
}

BRANCH_META: dict[str, dict[str, str]] = {
    "Aldeota": {"codigo": "72546"},
    "Sul": {"codigo": "74070"},
    "Bezerra": {"codigo": "488365"},
}

# =======================
# POSTGRES
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

# =======================
# DEBUG / STEPS
# =======================

def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


STEP_SCREENSHOTS = _bool_env("STEP_SCREENSHOTS", False)
STEP_SAVE_HTML = _bool_env("STEP_SAVE_HTML", False)
HEADLESS = _bool_env("HEADLESS", True)

SKIP_HOLIDAYS = _bool_env("SKIP_HOLIDAYS", True)
NO_FORTALEZA_MUNICIPAL = _bool_env("NO_FORTALEZA_MUNICIPAL", False)
INCLUDE_FORTALEZA_MUNICIPAL = not NO_FORTALEZA_MUNICIPAL

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
        print(f"   Print salvo: {png_path}")
    except Exception as e:
        print(f"   Falha ao salvar print: {e}")

    if save_html:
        try:
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print(f"   HTML salvo: {html_path}")
        except Exception as e:
            print(f"   Falha ao salvar HTML: {e}")


def step(msg: str, driver=None, snap_label: str | None = None, force_snap: bool = False):
    global _STEP_COUNTER
    _STEP_COUNTER += 1
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{_STEP_COUNTER:03d}] {now} - {msg}")

    if driver and (STEP_SCREENSHOTS or force_snap):
        label = snap_label or msg
        take_snapshot(driver, label=label, save_html=STEP_SAVE_HTML)


def _parse_cli_args():
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--inicio", "--start", dest="inicio", default=None,
                   help="Data inicial (dd/mm/aaaa ou aaaa-mm-dd)")
    p.add_argument("--fim", "--end", dest="fim", default=None,
                   help="Data final (dd/mm/aaaa ou aaaa-mm-dd)")
    p.add_argument("--no-email", dest="no_email", action="store_true",
                   help="Nao envia e-mail (forca SEND_EMAIL=False)")
    return p.parse_args()


def _parse_date_any(s: str) -> date:
    s = (s or "").strip()
    if not s:
        raise ValueError("data vazia")
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
        return datetime.strptime(s, "%d/%m/%Y").date()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return datetime.strptime(s, "%Y-%m-%d").date()
    raise ValueError(f"Formato invalido: {s} (use dd/mm/aaaa ou aaaa-mm-dd)")


# =======================
# HELPERS (FERIADOS)
# =======================

def easter_date_gregorian(year: int) -> date:
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
    for y in years:
        easter = easter_date_gregorian(y)
        mapping = {
            50: "Carnaval (Sabado)",
            49: "Carnaval (Domingo)",
            48: "Carnaval (Segunda)",
            47: "Carnaval (Terca)",
            46: "Quarta-feira de Cinzas",
        }
        for delta_days, name in mapping.items():
            hol[easter - timedelta(days=delta_days)] = name


def make_holiday_checker(years: List[int], include_fortaleza_municipal: bool = True):
    years = sorted(set([y for y in years if y is not None]))
    if not years:
        years = [datetime.now().year]

    br_ce = holidays.Brazil(subdiv="CE", years=years)

    if include_fortaleza_municipal:
        for y in years:
            br_ce[date(y, 4, 13)] = "Aniversario de Fortaleza"
            br_ce[date(y, 8, 15)] = "Nossa Senhora da Assuncao (Fortaleza)"
            br_ce[date(y, 12, 8)] = "Nossa Senhora da Conceicao (Fortaleza)"

    add_carnaval_and_ash_wednesday(br_ce, years)
    return br_ce


# =======================
# HELPERS (E-MAIL)
# =======================

def email_configurada() -> bool:
    return bool((EMAIL_FROM or EMAIL_USER) and EMAIL_PASSWORD)


def _resolve_sender() -> str:
    sender = (EMAIL_FROM or EMAIL_USER or "").strip()
    if not sender:
        raise RuntimeError("Remetente ausente. Defina EMAIL_FROM ou EMAIL_USER no .env.")
    if "@" not in sender:
        raise RuntimeError(f"Remetente invalido: '{sender}'. Informe um e-mail valido.")
    return sender


def enviar_email(subject: str, html_body: str, attachments: list[str] | None = None):
    attachments = attachments or []

    from_addr = _resolve_sender()
    login_user = (EMAIL_USER or from_addr)
    if not EMAIL_PASSWORD:
        raise RuntimeError("EMAIL_PASSWORD nao definido.")

    msg = MIMEMultipart()
    msg["From"] = formataddr(("Class Panel Bot", from_addr))
    msg["To"] = ", ".join(DESTINATARIOS)
    if CC:
        msg["Cc"] = ", ".join(CC)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    for path in attachments:
        if not os.path.exists(path):
            print(f"Anexo nao encontrado: {path}")
            continue
        with open(path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(path))
        part["Content-Disposition"] = f'attachment; filename="{os.path.basename(path)}"'
        msg.attach(part)

    all_rcpts = list(dict.fromkeys((DESTINATARIOS or []) + (CC or [])))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(login_user, EMAIL_PASSWORD)
        server.sendmail(from_addr, all_rcpts, msg.as_string())

    print(f"Email enviado de {from_addr} para: {', '.join(all_rcpts)}")


def montar_corpo_html_100(df100: pd.DataFrame, hoje_brt: pd.Timestamp, anexo_path: str) -> str:
    if df100.empty:
        return f"""
        <p>Ola,</p>
        <p>Nao foram encontradas turmas com <strong>100% de presenca</strong> no periodo considerado.</p>
        <p>Data de geracao: <strong>{hoje_brt:%d/%m/%Y %H:%M}</strong></p>
        <p>Anexo: <em>{os.path.basename(anexo_path)}</em></p>
        """

    tbl = df100.copy()
    tbl["Data"] = tbl["Data_dt"].dt.strftime("%d/%m/%Y")
    cols = [c for c in ["Data", "Sede", "Turma", "Curso", "Professor", "Integrantes", "Horario"] if c in tbl.columns]
    tabela_html = tbl[cols].to_html(index=False, border=0, justify="left")

    return f"""
    <p>Ola,</p>
    <p>Segue abaixo o relatorio de turmas com <strong>100% de presenca</strong> (sem faltas):</p>
    {tabela_html}
    <p>Anexo: <em>{os.path.basename(anexo_path)}</em></p>
    <p>Gerado em: <strong>{hoje_brt:%d/%m/%Y %H:%M}</strong></p>
    """


def enviar_relatorio_turmas_100(df100: pd.DataFrame, anexo_path: str):
    if not SEND_EMAIL or not email_configurada():
        print("E-mail desativado ou credenciais ausentes. Relatorio 100% presenca foi pulado.")
        return

    hoje_brt = pd.Timestamp.now(tz=TZ_NAME)
    if df100.empty:
        assunto = f"[Relatorio] Turmas 100% presenca - nenhum registro ({hoje_brt:%d/%m/%Y})"
    else:
        ultimo_dia = df100["Data_dt"].max()
        assunto = f"[Relatorio] Turmas 100% presenca - ate {ultimo_dia:%d/%m/%Y}"

    corpo_html = montar_corpo_html_100(df100, hoje_brt, anexo_path)
    enviar_email(assunto, corpo_html, attachments=[anexo_path])


# =======================
# HELPERS (SELENIUM)
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
    if _is_displayed_safe(driver, By.ID, "processing-modal"):
        return True

    if _is_displayed_safe(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_upgProcessando1_upg"):
        return True

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


def wait_for_postback(driver, timeout: int = 25):
    end = time.time() + timeout
    while time.time() < end:
        try:
            wait_overlay_gone(driver, timeout=3)
        except Exception:
            pass

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

        try:
            active = driver.execute_script("return (window.jQuery && jQuery.active) ? jQuery.active : 0;")
            if isinstance(active, (int, float)) and active != 0:
                time.sleep(0.2)
                continue
        except Exception:
            pass

        try:
            rs = driver.execute_script("return document.readyState")
            if rs == "complete":
                return
        except Exception:
            pass

        time.sleep(0.2)


def ensure_checkbox_state(driver, by, locator, desired: bool = True, timeout: int = 25):
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
        print(f"Screenshot salvo: {png_path}")
    except Exception as e:
        print(f"Falha ao salvar screenshot: {e}")

    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print(f"HTML salvo: {html_path}")
    except Exception as e:
        print(f"Falha ao salvar HTML: {e}")


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

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(90)
    driver.set_script_timeout(60)

    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": download_dir})
    except Exception as e:
        print(f"Nao consegui setar download behavior via CDP: {e}")

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

    raise TimeoutException(f"Timeout esperando novo download .xls em {download_dir}. Ultimo visto: {last_seen}")


def move_downloaded_file_unique(downloaded_path: str, target_dir: str, current_date: date, head_office: str) -> str:
    filename = f"Relatorio_{current_date.strftime('%d_%m_%Y')}_{head_office}.xls"
    target_path = os.path.join(target_dir, filename)
    shutil.move(downloaded_path, target_path)
    print(f"XLS movido ({head_office} | {current_date:%d/%m/%Y}) -> {target_path}")
    return target_path


# =======================
# LOGICA DE NEGOCIO
# =======================

def delete_turmas_invalidas_postgres():
    step("Removendo turmas invalidas do PostgreSQL", None)

    sql = f'''
    DELETE FROM "{DB_SCHEMA}"."{DB_TABLE}"
    WHERE
        turma ILIKE 'GT%%'
        OR (
            LENGTH(TRIM(turma)) >= 3
            AND SUBSTRING(UPPER(TRIM(turma)) FROM 3 FOR 1) = 'L'
        );
    '''

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(sql)
            deleted = cur.rowcount
        conn.commit()
        print(f"Remocao concluida no PostgreSQL: {deleted} registro(s) excluido(s).")
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def processar_turma(nome_turma: str | None):
    if not isinstance(nome_turma, str):
        return None

    nome_norm = nome_turma.strip()
    nome_lower = nome_norm.lower()

    turmas_ignoradas = ["aulas diversas", "aulas diversas 2", "aulas diversas gt"]
    if any(turma in nome_lower for turma in turmas_ignoradas):
        print(f"Turma ignorada: {nome_turma}")
        return None

    # Remove turmas que comecem com GT
    if nome_norm.upper().startswith("GT"):
        print(f"Turma ignorada (comeca com GT): {nome_turma}")
        return None

    # Remove turmas cuja terceira letra seja L (online)
    # Ex.: FSL...
    if len(nome_norm) >= 3 and nome_norm[2].upper() == "L":
        print(f"Turma ignorada (3a letra = L): {nome_turma}")
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
        return "Gerente de Projetos Ageis"
    elif nome_turma.startswith("FSL"):
        return "Full Stack Live"
    elif nome_turma.startswith("GT"):
        return "Geracao Tech"
    return ""


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
# SPONTE -> XLS
# =======================

def login_sponte(driver) -> Tuple[str, str]:
    step("Abrindo pagina de login (Sponte Home)", driver, "login_abrindo_home")
    driver.get(url_home)
    wait_ready(driver, timeout=30)
    step("Pagina de login carregada", driver, "login_home_carregada")

    step("Preenchendo e-mail (txtLogin)", driver, "login_preenchendo_email")
    safe_send_keys(driver, By.ID, "txtLogin", SPONTE_EMAIL, timeout=20)

    step("Preenchendo senha (txtSenha)", driver, "login_preenchendo_senha")
    safe_send_keys(driver, By.ID, "txtSenha", SPONTE_PASSWORD, timeout=20)

    step("Clicando em entrar (btnok)", driver, "login_click_btnok")
    safe_click(driver, By.ID, "btnok", timeout=25)

    step("Aguardando pos-login", driver, "login_pos_login_aguardando")
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
        print(f"Sede atual (label): {nome_empresa} | Codigo cliente: {cod_cliente}")
    except Exception:
        pass

    step("Login concluido", driver, "login_concluido")
    return nome_empresa, cod_cliente


def _get_checkbox_label_text(driver, checkbox_id: str) -> str:
    try:
        lab = driver.find_element(By.CSS_SELECTOR, f"label[for='{checkbox_id}']")
        return (lab.text or "").strip()
    except Exception:
        return ""


def selecionar_empresas_por_sede(driver, head_office: str):
    if head_office not in HEAD_OFFICES:
        raise ValueError(f"Sede invalida: {head_office}")

    step(f"Abrindo aba Empresas para selecionar sede: {head_office}", driver, f"empresas_abrindo_{head_office}")
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_liEmpresas", timeout=25)
    wait_for_postback(driver, timeout=25)
    time.sleep(0.5)
    step("Aba Empresas aberta", driver, f"empresas_aberta_{head_office}")

    cbs = driver.find_elements(By.CSS_SELECTOR, "input[id^='ctl00_ctl00_ContentPlaceHolder1_cblEmpresas_']")
    cb_ids = [cb.get_attribute("id") for cb in cbs if cb.get_attribute("id")]
    if not cb_ids:
        raise RuntimeError("Nao encontrei checkboxes de empresas (cblEmpresas_*).")

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

    for cid in cb_ids:
        if cid == alvo_id:
            continue
        ensure_checkbox_state(driver, By.ID, cid, desired=False, timeout=30)

    ensure_checkbox_state(driver, By.ID, alvo_id, desired=True, timeout=30)

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
        print(f"Validacao: checkboxes selecionados={len(selected)} -> {selected}")
    else:
        print(f"Selecao de empresa OK: {selected[0]}")

    step("Voltando para aba principal", driver, f"empresas_voltando_principal_{head_office}")
    safe_click(driver, By.CSS_SELECTOR, "ul.nav.nav-pills li:first-child", timeout=25)
    wait_for_postback(driver, timeout=25)
    time.sleep(0.5)


def configurar_filtros_frequencia(driver, current_date: date):
    step("Selecionando Situacao = Vigente", driver, f"filtros_situacao_vigente_{current_date:%d_%m_%Y}")
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
        raise TimeoutException(f"Nao consegui selecionar dia da semana no filtro: {dia_pt}")

    time.sleep(0.6)

    ensure_checkbox_state(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkRelatorioQuantitativo",
        desired=True,
        timeout=25,
    )

    ensure_checkbox_state(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkMarcarTurmas",
        desired=True,
        timeout=25,
    )
    time.sleep(0.8)

    date_str = current_date.strftime("%d/%m/%Y")

    start_el = safe_find(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_wcdDataInicioFaltasCons_txtData",
        timeout=25,
    )
    js_set_value_and_events(driver, start_el, date_str)

    end_el = safe_find(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_wcdDataTerminoFaltasCons_txtData",
        timeout=25,
    )
    js_set_value_and_events(driver, end_el, date_str)
    time.sleep(0.6)

    ensure_checkbox_state(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkExportar", desired=True, timeout=25)
    time.sleep(0.5)

    safe_click(driver, By.ID, "select2-ctl00_ctl00_ContentPlaceHolder1_cmbTipoExportacao-container", timeout=25)
    time.sleep(0.5)
    safe_click(driver, By.XPATH, "//*[normalize-space(text())='Excel Sem Formatação']", timeout=25)
    time.sleep(0.6)


def baixar_relatorio(driver, current_date: date, head_office: str) -> str:
    step("Capturando lista de arquivos antes do download", driver, f"download_before_{head_office}_{current_date:%d_%m_%Y}")
    before = set(os.listdir(download_dir))

    step("Clicando em Gerar relatorio", driver, f"download_click_gerar_{head_office}_{current_date:%d_%m_%Y}")
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_btnGerar_div", timeout=30)
    time.sleep(2)

    downloaded_path = wait_for_new_download_xls(download_dir, before_files=before, timeout=180)
    step(f"Download concluido: {os.path.basename(downloaded_path)}", driver, f"download_done_{head_office}_{current_date:%d_%m_%Y}")

    target_path = move_downloaded_file_unique(downloaded_path, base_target_dir, current_date, head_office)
    return target_path


def extrair_df_relatorio(xls_file_path: str, current_date: date, head_office: str) -> pd.DataFrame:
    print(f"Lendo XLS: {xls_file_path}")
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
        print(f"Sem turmas validas ({head_office} | {current_date:%d/%m/%Y}).")
        return pd.DataFrame()

    if "DataInicio" in data.columns:
        data["DataInicio"] = pd.to_datetime(data["DataInicio"], dayfirst=True, errors="coerce")
        hoje_brt = pd.Timestamp.now(tz=TZ_NAME).date()
        data = data.dropna(subset=["DataInicio"])
        data = data[data["DataInicio"].dt.date <= hoje_brt].copy()

    if data.empty:
        print(f"Sem registros apos filtro DataInicio<=hoje ({head_office} | {current_date:%d/%m/%Y}).")
        return pd.DataFrame()

    data["Data"] = current_date.strftime("%d/%m/%Y")
    data["Curso"] = data["Nome"].apply(detectar_curso) if "Nome" in data.columns else ""

    if "Nome" in data.columns:
        data["Sede_detectada"] = data["Nome"].apply(lambda n: detectar_sede_por_nome_turma(str(n), default=""))
        data["Sede"] = data["Sede_detectada"].where(data["Sede_detectada"].astype(str).str.strip() != "", head_office)

        diverg = data[(data["Sede_detectada"].astype(str).str.strip() != "") & (data["Sede_detectada"] != head_office)]
        if not diverg.empty:
            print(f"{len(diverg)} linha(s) no XLS de {head_office} parecem ser de outra sede.")
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
        raise ValueError(f"Intervalo invalido: inicio {start_date_range:%d/%m/%Y} > fim {end_date_range:%d/%m/%Y}")

    holiday_set = None
    if SKIP_HOLIDAYS:
        years = list(range(start_date_range.year, end_date_range.year + 1))
        holiday_set = make_holiday_checker(years, include_fortaleza_municipal=INCLUDE_FORTALEZA_MUNICIPAL)

    print("=" * 90)
    print(f"RUN_ID: {RUN_ID}")
    print(f"Prints: {'ON' if STEP_SCREENSHOTS else 'OFF'} | HTML: {'ON' if STEP_SAVE_HTML else 'OFF'}")
    print(f"Headless: {'ON' if HEADLESS else 'OFF'}")
    print(f"Pasta debug: {DEBUG_RUN_DIR}")
    print(f"Janela: {start_date_range:%d/%m/%Y} -> {end_date_range:%d/%m/%Y} (BRT)")
    print(f"Pular feriados: {'SIM' if SKIP_HOLIDAYS else 'NAO'} | Fortaleza municipal: {'SIM' if INCLUDE_FORTALEZA_MUNICIPAL else 'NAO'}")
    print("=" * 90)

    combined_data: list[pd.DataFrame] = []

    current_date = start_date_range
    while current_date <= end_date_range:
        if current_date.weekday() == 6:
            print(f"Pulando Domingo: {current_date:%d/%m/%Y}")
            current_date += timedelta(days=1)
            continue

        if SKIP_HOLIDAYS and holiday_set is not None and current_date in holiday_set:
            motivo = str(holiday_set.get(current_date, "Feriado")).strip() or "Feriado"
            print(f"Pulando FERIADO: {current_date:%d/%m/%Y} - {motivo}")
            current_date += timedelta(days=1)
            continue

        for head_office in HEAD_OFFICES:
            success = False
            for attempt in range(1, MAX_ATTEMPTS + 1):
                user_data_dir = tempfile.mkdtemp(prefix="chrome_profile_")
                driver = None
                label = f"{current_date.strftime('%d_%m_%Y')}_{head_office}_attempt_{attempt}"

                try:
                    step(f"INICIO: {current_date:%d/%m/%Y} | {head_office} | tentativa {attempt}/{MAX_ATTEMPTS}")

                    driver = build_driver(download_dir=download_dir, user_data_dir=user_data_dir)
                    step("Driver criado", driver, f"driver_ok_{label}")

                    login_sponte(driver)

                    step("Abrindo URL do relatorio didatico", driver, f"goto_didatico_{label}")
                    driver.get(url_didatico)
                    wait_ready(driver, timeout=30)
                    step("Pagina do relatorio carregada", driver, f"didatico_ok_{label}")

                    try:
                        driver.execute_script("document.body.style.zoom='100%'")
                    except Exception:
                        pass

                    selecionar_empresas_por_sede(driver, head_office=head_office)
                    configurar_filtros_frequencia(driver, current_date=current_date)

                    xls_path = baixar_relatorio(driver, current_date=current_date, head_office=head_office)

                    step("Extraindo dados do XLS", driver, f"extract_df_{label}")
                    df = extrair_df_relatorio(xls_path, current_date=current_date, head_office=head_office)

                    if not df.empty:
                        combined_data.append(df)
                        step(f"Dados adicionados ({head_office} | {current_date:%d/%m/%Y})", driver, f"df_added_{label}")
                    else:
                        step(f"Sem dados para adicionar ({head_office} | {current_date:%d/%m/%Y})", driver, f"df_empty_{label}")

                    success = True
                    step(f"FIM OK: {current_date:%d/%m/%Y} | {head_office}", driver, f"fim_ok_{label}")
                    break

                except Exception as e:
                    step(f"Erro ({head_office} | {current_date:%d/%m/%Y}): {e}", driver, f"error_{label}", force_snap=True)
                    if driver:
                        try:
                            take_debug_snapshot(driver, label=f"error_{label}")
                        except Exception:
                            pass

                finally:
                    step("Encerrando driver e limpando perfil temporario", driver, f"cleanup_{label}")
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
                print(f"Falha apos {MAX_ATTEMPTS} tentativas: {head_office} | {current_date:%d/%m/%Y}")

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
    print(f"Combined data salvo: {COMBINED_PATH}")

    return COMBINED_PATH


# =======================
# RELATORIO 100% PRESENCA
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
        raise KeyError(f"Colunas obrigatorias ausentes: {faltando} - tenho {sorted(cols)}")

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
    step("Gerando relatorio de 100% presenca", None)
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

    step(f"Salvando anexo 100% presenca em: {anexo_path}", None)
    if not df_100.empty:
        temp = df_100.copy()
        temp["Data"] = temp["Data_dt"].dt.strftime("%d/%m/%Y")
        temp.drop(columns=["Data_dt"], inplace=True)
        temp.to_excel(anexo_path, index=False)
    else:
        pd.DataFrame(columns=["Data", "Turma", "Curso", "Professor", "Integrantes", "Horario", "Sede"]).to_excel(
            anexo_path, index=False
        )

    step("Enviando e-mail do relatorio 100% presenca", None)
    enviar_relatorio_turmas_100(df_100, anexo_path)
    step("Relatorio 100% presenca finalizado", None)
    return anexo_path


# =======================
# POSTGRES HELPERS
# =======================

def _validate_db_env():
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


def get_db_connection():
    _validate_db_env()
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


def normalize_dataframe_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "data_aula", "turma", "curso", "professor", "vagas", "integrantes",
            "trancados", "horario", "nao_frequente", "frequente", "dias_semana", "sede"
        ])

    data = df.copy()
    data.columns = [c.strip() for c in data.columns]

    rename_map = {
        "Nome": "Turma",
        "Não Frequentes": "NaoFrequente",
        "Frequentes": "Frequente",
        "Dias da Semana": "DiasSemana",
    }
    for src, dst in rename_map.items():
        if src in data.columns and dst not in data.columns:
            data.rename(columns={src: dst}, inplace=True)

    required = ["Data", "Turma"]
    missing = [c for c in required if c not in data.columns]
    if missing:
        raise RuntimeError(f"Colunas obrigatorias ausentes para sync no Postgres: {missing}")

    for c in ["Vagas", "Integrantes", "Trancados", "NaoFrequente", "Frequente"]:
        if c not in data.columns:
            data[c] = None
        data[c] = pd.to_numeric(data[c], errors="coerce")

    for c in ["Curso", "Professor", "Horario", "DiasSemana", "Sede"]:
        if c not in data.columns:
            data[c] = ""

    data["Data"] = pd.to_datetime(data["Data"], dayfirst=True, errors="coerce")
    data = data.dropna(subset=["Data"]).copy()

    data["Sede_calc"] = data["Turma"].astype(str).apply(lambda n: detectar_sede_por_nome_turma(n, default=""))
    data["Sede"] = data["Sede_calc"].where(data["Sede_calc"].astype(str).str.strip() != "", data["Sede"])
    data.drop(columns=["Sede_calc"], inplace=True)

    data = data[[
        "Data", "Turma", "Curso", "Professor", "Vagas", "Integrantes",
        "Trancados", "Horario", "NaoFrequente", "Frequente", "DiasSemana", "Sede"
    ]].copy()

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

    for col in ["curso", "professor", "horario", "dias_semana", "sede", "turma"]:
        data[col] = data[col].apply(lambda x: None if pd.isna(x) or str(x).strip() == "" else str(x).strip())

    for col in ["vagas", "integrantes", "trancados", "nao_frequente", "frequente"]:
        data[col] = data[col].apply(lambda x: None if pd.isna(x) else int(x))

    data = data.drop_duplicates(subset=["data_aula", "turma"], keep="last").reset_index(drop=True)
    return data


def upsert_frequencia_postgres(df: pd.DataFrame):
    step("Preparando sincronizacao com PostgreSQL", None)

    data = normalize_dataframe_for_postgres(df)
    if data.empty:
        print("Nenhum dado valido para enviar ao PostgreSQL.")
        return

    rows = list(data.itertuples(index=False, name=None))

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

    conn = None
    try:
        conn = get_db_connection()
        step("Conexao com PostgreSQL estabelecida", None)

        if AUTO_CREATE_TABLE:
            step("Garantindo schema/tabela no PostgreSQL", None)
            ensure_postgres_schema_and_table(conn)

        with conn.cursor() as cur:
            execute_values(
                cur,
                sql,
                rows,
                page_size=500
            )
        conn.commit()

        print(f"UPSERT concluido no PostgreSQL: {len(rows)} registro(s) processado(s).")
        print(f"Destino: {DB_SCHEMA}.{DB_TABLE}")

    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def postgres_sync_frequencia(input_file: str):
    step("Iniciando sync com PostgreSQL (lendo Excel consolidado)", None)
    df = pd.read_excel(input_file)
    df.columns = [c.strip() for c in df.columns]

    upsert_frequencia_postgres(df)
    delete_turmas_invalidas_postgres()

    step("Sincronizacao PostgreSQL concluida", None)


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
    step("Coleta Sponte concluida (Excel consolidado pronto)", None)

    if SEND_EMAIL:
        gerar_e_enviar_100_presenca(output_path)
        step("Relatorio 100% presenca concluido", None)
    else:
        step("Modo NO-EMAIL ativo: pulando relatorio 100% presenca", None)

    postgres_sync_frequencia(output_path)

    step("FIM do pipeline (tudo concluido)", None)
    print(f"Debug/prints desta execucao: {DEBUG_RUN_DIR}")


if __name__ == "__main__":
    main()
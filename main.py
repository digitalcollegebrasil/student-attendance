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

DEBUG (NOVO):
    STEP_SCREENSHOTS=1 (default: 1)  -> tira prints em cada passo
    STEP_SAVE_HTML=0    (default: 0) -> salva HTML em cada passo (pode pesar)
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
# SEND_EMAIL pode ser:
#   - "1"/"true"/"yes"  -> força enviar
#   - "0"/"false"/"no"  -> desativa enviar
#   - "auto" (padrão)   -> envia só se EMAIL_USER e EMAIL_PASSWORD existirem
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
GOOGLE_SHEET_ID_FREQ = "19_bvzaFfHkHWlRi4dV7hEJ44W2LoJIOSJkWeWW7CQ4A"

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
# DEBUG / STEPS (NOVO)
# =======================

def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

STEP_SCREENSHOTS = _bool_env("STEP_SCREENSHOTS", False)  # default OFF
STEP_SAVE_HTML = _bool_env("STEP_SAVE_HTML", False)     # default OFF (pesa)

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
      python3 main.py --no-email  (usa janela padrão START_DAYS_AGO/END_DAYS_AGO)
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
    # aceita dd/mm/aaaa ou aaaa-mm-dd
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
        return datetime.strptime(s, "%d/%m/%Y").date()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return datetime.strptime(s, "%Y-%m-%d").date()
    raise ValueError(f"Formato inválido: {s} (use dd/mm/aaaa ou aaaa-mm-dd)")


# =======================
# HELPERS (e-mail)
# =======================

def email_configurada() -> bool:
    return bool((EMAIL_FROM or EMAIL_USER) and EMAIL_PASSWORD)


def _resolve_sender() -> str:
    sender = (EMAIL_FROM or EMAIL_USER or "").strip()
    if not sender:
        raise RuntimeError(
            "Remetente ausente. Defina EMAIL_FROM ou EMAIL_USER no .env (ex.: EMAIL_USER='seuemail@dominio')."
        )
    if "@" not in sender:
        raise RuntimeError(f"Remetente inválido: '{sender}'. Informe um e-mail válido.")
    return sender


def enviar_email(subject: str, html_body: str, attachments: list[str] | None = None):
    attachments = attachments or []

    FROM_ADDR = _resolve_sender()
    LOGIN_USER = (EMAIL_USER or FROM_ADDR)
    if not EMAIL_PASSWORD:
        raise RuntimeError("EMAIL_PASSWORD não definido. Crie/app password e defina no .env.")

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
        print("E-mail desativado ou credenciais ausentes (EMAIL_USER/EMAIL_PASSWORD). Relatório 100% presença foi pulado.")
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
            time.sleep(0.2)
            try:
                el.click()
            except (ElementClickInterceptedException, WebDriverException):
                driver.execute_script("arguments[0].click();", el)
            return
        except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException, WebDriverException) as e:
            last_exc = e
            time.sleep(1)

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


def take_debug_snapshot(driver, label: str):
    # Mantido para compatibilidade; agora usa a pasta da execução (DEBUG_RUN_DIR)
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

    # HEADLESS "server safe"
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

    # perfil temporário
    chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
    service = Service()
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.set_page_load_timeout(90)
    driver.set_script_timeout(60)

    # garante downloads em headless (CDP)
    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": download_dir})
    except Exception as e:
        print(f"⚠️ Não consegui setar download behavior via CDP (pode ainda funcionar): {e}")

    return driver


def wait_for_new_download_xls(download_dir: str, before_files: set[str], timeout=120) -> str:
    """
    Espera aparecer um NOVO .xls (não presente em before_files) e garante que não exista .crdownload pendente.
    Retorna o caminho do arquivo novo mais recente.
    """
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
# LÓGICA DE NEGÓCIO (data/curso)
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


def weekday_pt_for_filter(d: date) -> str:
    # 0=Mon ... 6=Sun
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

def login_sponte(driver):
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
    step("Login concluído (página pronta)", driver, "login_concluido")


def selecionar_empresas_por_sede(driver, head_office: str):
    """
    Seleciona empresas (sede) na aba Empresas.
    Estratégia:
      - abre aba Empresas
      - usa chkMarcarTodas para marcar tudo e depois desmarcar tudo (limpar)
      - marca somente a sede alvo (cblEmpresas_{idx})
      - volta para a primeira aba/pill (li:first-child)
    """
    idx_map = {"Aldeota": 0, "Sul": 1, "Bezerra": 2}
    if head_office not in idx_map:
        raise ValueError(f"Sede inválida: {head_office}")

    step(f"Abrindo aba Empresas para selecionar sede: {head_office}", driver, f"empresas_abrindo_{head_office}")
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_liEmpresas", timeout=25)
    time.sleep(1)
    step("Aba Empresas aberta", driver, f"empresas_aberta_{head_office}")

    # Tenta "limpar seleção": marcar todas e desmarcar todas (toggle comum)
    step("Tentando limpar seleção (marcar/desmarcar todas)", driver, f"empresas_limpando_{head_office}")
    try:
        safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkMarcarTodas", timeout=25)
        time.sleep(0.8)
        safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkMarcarTodas", timeout=25)
        time.sleep(0.8)
    except Exception:
        step("Checkbox 'Marcar todas' não encontrado (seguindo mesmo assim)", driver, f"empresas_sem_marcar_todas_{head_office}")

    # Marca somente a sede alvo
    idx = idx_map[head_office]
    chk_id = f"ctl00_ctl00_ContentPlaceHolder1_cblEmpresas_{idx}"

    step(f"Marcando somente a sede alvo: {head_office} (id={chk_id})", driver, f"empresas_marcando_{head_office}")
    el = safe_find(driver, By.ID, chk_id, timeout=25)
    try:
        is_checked = el.is_selected()
    except Exception:
        is_checked = False

    if not is_checked:
        safe_click(driver, By.ID, chk_id, timeout=25)
        time.sleep(0.8)

    step(f"Sede marcada: {head_office}", driver, f"empresas_sede_ok_{head_office}")

    # volta para a primeira pill/aba
    step("Voltando para aba principal (primeira pill)", driver, f"empresas_voltando_principal_{head_office}")
    safe_click(driver, By.CSS_SELECTOR, "ul.nav.nav-pills li:first-child", timeout=25)
    time.sleep(1)
    step("Aba principal selecionada", driver, f"empresas_principal_ok_{head_office}")


def configurar_filtros_frequencia(driver, current_date: date):
    """
    Configura:
      - Situação = Vigente
      - Dia da semana = current_date
      - Relatório Quantitativo = ON
      - Marcar turmas = ON
      - Data início = Data término = current_date
      - Exportar = ON (Excel Sem Formatação)
    """
    step("Selecionando Situação = Vigente", driver, f"filtros_situacao_vigente_{current_date:%d_%m_%Y}")
    safe_select_by_visible_text(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_cmbSituacaoTurma",
        "Vigente",
        timeout=25,
    )
    time.sleep(0.8)
    step("Situação selecionada: Vigente", driver, f"filtros_situacao_ok_{current_date:%d_%m_%Y}")

    # Dia da semana
    dia_pt = weekday_pt_for_filter(current_date)
    step(f"Abrindo seletor de dia da semana e escolhendo: {dia_pt}", driver, f"filtros_dia_abre_{dia_pt}_{current_date:%d_%m_%Y}")
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_divDiaSemana", timeout=25)
    time.sleep(0.6)

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

    time.sleep(0.8)
    step(f"Dia da semana selecionado: {dia_pt}", driver, f"filtros_dia_ok_{dia_pt}_{current_date:%d_%m_%Y}")

    # Relatório Quantitativo
    step("Marcando Relatório Quantitativo", driver, f"filtros_relatorio_quant_{current_date:%d_%m_%Y}")
    safe_click(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkRelatorioQuantitativo",
        timeout=25,
    )
    time.sleep(0.6)
    step("Relatório Quantitativo marcado", driver, f"filtros_relatorio_quant_ok_{current_date:%d_%m_%Y}")

    # Marcar todas as turmas
    step("Marcando 'Marcar turmas' (todas)", driver, f"filtros_marcar_turmas_{current_date:%d_%m_%Y}")
    safe_click(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkMarcarTurmas",
        timeout=25,
    )
    time.sleep(1)
    step("Turmas marcadas", driver, f"filtros_marcar_turmas_ok_{current_date:%d_%m_%Y}")

    date_str = current_date.strftime("%d/%m/%Y")

    # Inputs de data (JS para evitar mascaras/readonly)
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

    time.sleep(0.8)
    step("Datas configuradas", driver, f"filtros_datas_ok_{current_date:%d_%m_%Y}")

    # Exportar checkbox
    step("Marcando 'Exportar'", driver, f"filtros_exportar_chk_{current_date:%d_%m_%Y}")
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkExportar", timeout=25)
    time.sleep(0.6)
    step("Exportar marcado", driver, f"filtros_exportar_chk_ok_{current_date:%d_%m_%Y}")

    # Select2: Tipo Exportação (Excel Sem Formatação)
    step("Abrindo tipo de exportação (select2) e escolhendo 'Excel Sem Formatação'", driver, f"filtros_tipo_export_{current_date:%d_%m_%Y}")
    safe_click(driver, By.ID, "select2-ctl00_ctl00_ContentPlaceHolder1_cmbTipoExportacao-container", timeout=25)
    time.sleep(0.6)
    safe_click(driver, By.XPATH, "//*[normalize-space(text())='Excel Sem Formatação']", timeout=25)
    time.sleep(0.8)
    step("Tipo de exportação selecionado: Excel Sem Formatação", driver, f"filtros_tipo_export_ok_{current_date:%d_%m_%Y}")


def baixar_relatorio(driver, current_date: date, head_office: str) -> str:
    """
    Clica em Gerar e espera download novo do .xls. Move para target com nome único.
    """
    step(f"Preparando para gerar relatório (capturando lista de arquivos antes do download)", driver, f"download_before_{head_office}_{current_date:%d_%m_%Y}")
    before = set(os.listdir(download_dir))

    step("Clicando em 'Gerar' relatório", driver, f"download_click_gerar_{head_office}_{current_date:%d_%m_%Y}")
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_btnGerar_div", timeout=30)
    time.sleep(2)

    step("Aguardando novo download .xls", driver, f"download_wait_{head_office}_{current_date:%d_%m_%Y}")
    downloaded_path = wait_for_new_download_xls(download_dir, before_files=before, timeout=180)
    step(f"Download concluído: {os.path.basename(downloaded_path)}", driver, f"download_done_{head_office}_{current_date:%d_%m_%Y}")

    step("Movendo arquivo baixado para target com nome único", driver, f"download_move_{head_office}_{current_date:%d_%m_%Y}")
    target_path = move_downloaded_file_unique(downloaded_path, base_target_dir, current_date, head_office)
    step(f"Arquivo movido: {os.path.basename(target_path)}", driver, f"download_moved_{head_office}_{current_date:%d_%m_%Y}")

    return target_path


def extrair_df_relatorio(xls_file_path: str, current_date: date, head_office: str) -> pd.DataFrame:
    """
    Lê o XLS e padroniza colunas para o combinado:
      Data, Nome, Curso, Professor, Vagas, Integrantes, Trancados, Horario,
      Não Frequentes, Frequentes, Dias da Semana, Sede
    """
    print(f"📄 Lendo XLS: {xls_file_path}")
    data = pd.read_excel(xls_file_path, skiprows=3)
    data.columns = [c.strip() for c in data.columns]

    # Normalizações de nomes (variações comuns)
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

    # Filtra turmas ignoradas
    if "Nome" in data.columns:
        data["Nome"] = data["Nome"].apply(processar_turma)
        data = data.dropna(subset=["Nome"])

    if data.empty:
        print(f"ℹ️ Sem turmas válidas ({head_office} | {current_date:%d/%m/%Y}).")
        return pd.DataFrame()

    # DataInicio <= hoje (BRT) (garante consistência)
    if "DataInicio" in data.columns:
        data["DataInicio"] = pd.to_datetime(data["DataInicio"], dayfirst=True, errors="coerce")
        hoje_brt = pd.Timestamp.now(tz=TZ_NAME).date()
        data = data.dropna(subset=["DataInicio"])
        data = data[data["DataInicio"].dt.date <= hoje_brt].copy()

    if data.empty:
        print(f"ℹ️ Sem registros após filtro DataInicio<=hoje ({head_office} | {current_date:%d/%m/%Y}).")
        return pd.DataFrame()

    # Enriquecimentos
    data["Data"] = current_date.strftime("%d/%m/%Y")
    data["Curso"] = data["Nome"].apply(detectar_curso) if "Nome" in data.columns else ""
    data["Sede"] = head_office

    # Numéricos
    for c in ["Frequentes", "Não Frequentes", "Integrantes", "Trancados", "Vagas"]:
        if c in data.columns:
            data[c] = pd.to_numeric(data[c], errors="coerce")

    # Remove linhas "lixo"
    if "Frequentes" in data.columns and "Não Frequentes" in data.columns:
        condicao_remover = (
            ((data["Frequentes"] == 0) & (data["Não Frequentes"] == 0)) |
            ((data["Frequentes"] == 0) & (data["Não Frequentes"].isin([1, 2]))) |
            ((data["Não Frequentes"] == 0) & (data["Frequentes"].isin([1, 2])))
        )
        data = data[~condicao_remover].copy()

    # Colunas finais do combinado
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

    print("=" * 90)
    print(f"🧭 RUN_ID: {RUN_ID}")
    print(f"🖼️ Prints em cada passo: {'ON' if STEP_SCREENSHOTS else 'OFF'}")
    print(f"📁 Pasta de debug desta execução: {DEBUG_RUN_DIR}")
    print(f"🗓️ Janela: {start_date_range:%d/%m/%Y} -> {end_date_range:%d/%m/%Y} (BRT)")
    print("=" * 90)

    combined_data: list[pd.DataFrame] = []

    current_date = start_date_range
    while current_date <= end_date_range:
        # pula domingo
        if current_date.weekday() == 6:
            print(f"⏭️ Pulando Domingo: {current_date:%d/%m/%Y}")
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

                    step("Construindo driver Chrome (headless) + CDP download", None)
                    driver = build_driver(download_dir=download_dir, user_data_dir=user_data_dir)
                    step("Driver criado", driver, f"driver_ok_{label}")

                    # LOGIN
                    login_sponte(driver)

                    # IR PARA RELATÓRIO
                    step("Abrindo URL do relatório didático (Turmas)", driver, f"goto_didatico_{label}")
                    driver.get(url_didatico)
                    wait_ready(driver, timeout=30)
                    step("Página do relatório carregada", driver, f"didatico_ok_{label}")

                    # Evita zoom/headless inconsistências
                    step("Ajustando zoom 100% (se possível)", driver, f"zoom_{label}")
                    try:
                        driver.execute_script("document.body.style.zoom='100%'")
                    except Exception:
                        pass
                    step("Zoom ajustado", driver, f"zoom_ok_{label}")

                    # Seleciona sede/empresa
                    selecionar_empresas_por_sede(driver, head_office=head_office)

                    # Configura filtros de frequência
                    configurar_filtros_frequencia(driver, current_date=current_date)

                    # Baixa relatório
                    xls_path = baixar_relatorio(driver, current_date=current_date, head_office=head_office)

                    # Extrai DF
                    step("Extraindo dados do XLS para DataFrame", driver, f"extract_df_{label}")
                    df = extrair_df_relatorio(xls_path, current_date=current_date, head_office=head_office)

                    if not df.empty:
                        print(df.head(10))
                        combined_data.append(df)
                        step(f"✅ Dados adicionados ({head_office} | {current_date:%d/%m/%Y}).", driver, f"df_added_{label}")
                    else:
                        step(f"ℹ️ Sem dados para adicionar ({head_office} | {current_date:%d/%m/%Y}).", driver, f"df_empty_{label}")

                    success = True
                    step(f"FIM OK: {current_date:%d/%m/%Y} | {head_office} | tentativa {attempt}", driver, f"fim_ok_{label}")
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

    # Salva combinado
    step("Consolidando DataFrames coletados", None)
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=[
            "Data", "Nome", "Curso", "Professor", "Vagas", "Integrantes", "Trancados",
            "Horario", "Não Frequentes", "Frequentes", "Dias da Semana", "Sede"
        ])

    # sobrescreve arquivo local
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

    # padroniza nomes
    df.rename(columns={
        "Nome": "Turma",
        "Frequentes": "Frequente",
        "Frequente": "Frequente",
        "NaoFrequente": "Não Frequentes",
        "DiasSemana": "Dias da Semana",
        "Data Início": "DataInicio",
    }, inplace=True)

    # ignora GT
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
    """
    Garante que exista 1 linha de cabeçalho.

    Retorna:
      (cabecalho, dados_existentes, data_start_row)

    - Se a planilha estiver vazia: cria cabeçalho na linha 1.
    - Se já existir cabeçalho na linha 1: usa.
    - Se existir cabeçalho na linha 2 (caso raro, planilha com título na linha 1): usa linha 2.
    - Se não encontrar cabeçalho: força cabeçalho na linha 1 (overwrites linha 1).
    """
    valores_existentes = sheet_destino.get_all_values()

    # Caso: totalmente vazia
    if not valores_existentes:
        sheet_destino.update("A1", [expected_header], value_input_option="RAW")
        cabecalho = expected_header
        dados_existentes = []
        data_start_row = 2
        print("🧾 Cabeçalho criado (planilha estava vazia).")
        return cabecalho, dados_existentes, data_start_row

    # Caso: header na linha 1
    if _row_matches_header(valores_existentes[0], expected_header):
        cabecalho = [c.strip() for c in valores_existentes[0][:len(expected_header)]]
        dados_existentes = valores_existentes[1:]
        data_start_row = 2
        return cabecalho, dados_existentes, data_start_row

    # Caso: header na linha 2 (título na linha 1)
    if len(valores_existentes) >= 2 and _row_matches_header(valores_existentes[1], expected_header):
        cabecalho = [c.strip() for c in valores_existentes[1][:len(expected_header)]]
        dados_existentes = valores_existentes[2:]
        data_start_row = 3
        return cabecalho, dados_existentes, data_start_row

    # Caso: não tem header no formato esperado -> força na linha 1
    sheet_destino.update("A1", [expected_header], value_input_option="RAW")
    valores_existentes = sheet_destino.get_all_values()
    cabecalho = expected_header
    dados_existentes = valores_existentes[1:] if len(valores_existentes) > 1 else []
    data_start_row = 2
    print("🧾 Cabeçalho forçado na linha 1 (não existia no formato esperado).")
    return cabecalho, dados_existentes, data_start_row


def atualizar_linhas(sheet_destino, df_novos: pd.DataFrame, colunas_numericas: list[str]):
    # ✅ Agora: aceita 1 cabeçalho, e cria/força cabeçalho se não tiver.
    cabecalho, dados_existentes, data_start_row = ensure_sheet_header(sheet_destino, SHEET_HEADER_FREQ)

    try:
        idx_data = cabecalho.index("Data")
        idx_turma = cabecalho.index("Turma")
    except ValueError as e:
        print(f"Erro ao localizar colunas no cabeçalho: {e}")
        return

    # Mapa de chave -> linha (1-based do Google Sheets)
    index_map = {}
    for i, linha in enumerate(dados_existentes):
        # normaliza tamanho da linha
        if len(linha) < len(cabecalho):
            linha = linha + [""] * (len(cabecalho) - len(linha))
        chave = (linha[idx_data], linha[idx_turma])
        if chave != ("", ""):
            index_map[chave] = i + data_start_row

    # Atualiza/insere
    for _, row in df_novos.iterrows():
        row = row.fillna("")

        # chave sempre por Data + Turma
        chave = (str(row.get("Data", "")), str(row.get("Turma", "")))

        # monta valores alinhados ao cabeçalho (evita depender da ordem do DF)
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

        time.sleep(0.8)


def detectar_linhas_divergentes_por_sheet(service_ro, spreadsheet_id: str, sheet_index: int, tipos_ideais: dict[int, str]) -> list[int]:
    """
    Lê gridData do sheet_index e identifica linhas onde o tipo efetivo diverge do ideal.
    tipos_ideais: {col_index_1based: "DATE"/"NUMBER"/"STRING"/...}
    """
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

        # Data -> ISO (garante tipagem DATE)
        if idx_data is not None and idx_data < len(linha):
            raw = linha[idx_data]
            if raw:
                dt = pd.to_datetime(raw, dayfirst=True, errors="coerce")
                if pd.isna(dt):
                    dt = pd.to_datetime(raw, errors="coerce")
                if pd.notna(dt):
                    linha[idx_data] = dt.strftime("%Y-%m-%d")

        # Numéricos -> número puro
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

    # Coluna A (Data) -> dd/MM/yyyy
    requests.append({
        "repeatCell": {
            "range": {"sheetId": worksheet.id, "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
            "fields": "userEnteredFormat.numberFormat"
        }
    })

    # Numéricas: E (4), F (5), G (6), I (8), J (9) -> 0
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

    # padroniza nomes para base
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

    # ignora GT
    df = df[~df["Turma"].astype(str).str.startswith("GT")].copy()

    # ✅ Ajusta nomes EXACTOS para o cabeçalho do Google Sheets solicitado
    df.rename(columns={
        "Não Frequentes": "NaoFrequente",
        "Dias da Semana": "DiasSemana",
    }, inplace=True)

    # Garante todas as colunas do header (mesmo vazias) e ordena
    for c in SHEET_HEADER_FREQ:
        if c not in df.columns:
            df[c] = ""
    df = df[SHEET_HEADER_FREQ].copy()

    colunas_numericas = ["Vagas", "Integrantes", "Trancados", "Frequente", "NaoFrequente"]
    for coluna in colunas_numericas:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    # separa online/presencial
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

    # Tipos ideais (por índice 1-based) — baseado no header solicitado:
    # A:Data (DATE), B:Turma (STRING), C:Curso (STRING), D:Professor (STRING),
    # E:Vagas (NUMBER), F:Integrantes (NUMBER), G:Trancados (NUMBER), H:Horario (STRING),
    # I:NaoFrequente (NUMBER), J:Frequente (NUMBER), K:DiasSemana (STRING), L:Sede (STRING)
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


# =======================
# MAIN
# =======================

def main():
    global SEND_EMAIL
    args = _parse_cli_args()

    start_dt = _parse_date_any(args.inicio) if args.inicio else None
    end_dt = _parse_date_any(args.fim) if args.fim else None

    # se passou intervalo manual, NÃO envia e-mail (mesmo sem --no-email)
    if args.no_email or args.inicio or args.fim:
        SEND_EMAIL = False

    step("START do pipeline", None)

    output_path = run_sponte_frequencia(start_date_range=start_dt, end_date_range=end_dt)
    step("Coleta Sponte concluída (Excel consolidado pronto)", None)

    if SEND_EMAIL:
        gerar_e_enviar_100_presenca(output_path)
        step("Relatório 100% presença concluído", None)
    else:
        step("Modo NO-EMAIL ativo: pulando relatório 100% presença (gerar/enviar)", None)

    google_sheets_sync_frequencia(output_path)
    step("FIM do pipeline (tudo concluído)", None)
    print(f"📌 Debug/prints desta execução: {DEBUG_RUN_DIR}")

if __name__ == "__main__":
    main()

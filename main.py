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
"""

from __future__ import annotations

import os
import re
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

# Sedes alvo
HEAD_OFFICES = ["Aldeota", "Sul", "Bezerra"]


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
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    png_path = os.path.join(DEBUG_DIR, f"{ts}_{label}.png")
    html_path = os.path.join(DEBUG_DIR, f"{ts}_{label}.html")

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

    service = Service(CHROMEDRIVER_PATH)
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
    driver.get(url_home)
    wait_ready(driver, timeout=30)

    safe_send_keys(driver, By.ID, "txtLogin", SPONTE_EMAIL, timeout=20)
    safe_send_keys(driver, By.ID, "txtSenha", SPONTE_PASSWORD, timeout=20)
    safe_click(driver, By.ID, "btnok", timeout=25)

    time.sleep(2)
    wait_ready(driver, timeout=30)


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

    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_liEmpresas", timeout=25)
    time.sleep(1)

    # Tenta "limpar seleção": marcar todas e desmarcar todas (toggle comum)
    try:
        safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkMarcarTodas", timeout=25)
        time.sleep(0.8)
        safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkMarcarTodas", timeout=25)
        time.sleep(0.8)
    except Exception:
        # Se não existir, seguimos sem quebrar.
        pass

    # Marca somente a sede alvo
    idx = idx_map[head_office]
    chk_id = f"ctl00_ctl00_ContentPlaceHolder1_cblEmpresas_{idx}"

    # garante "checked" via atributo selecionado (sem depender do toggle anterior)
    el = safe_find(driver, By.ID, chk_id, timeout=25)
    try:
        is_checked = el.is_selected()
    except Exception:
        is_checked = False

    if not is_checked:
        safe_click(driver, By.ID, chk_id, timeout=25)
        time.sleep(0.8)

    # volta para a primeira pill/aba
    safe_click(driver, By.CSS_SELECTOR, "ul.nav.nav-pills li:first-child", timeout=25)
    time.sleep(1)


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
    # Situação: Vigente (select padrão por trás do select2)
    safe_select_by_visible_text(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_cmbSituacaoTurma",
        "Vigente",
        timeout=25,
    )
    time.sleep(0.8)

    # Dia da semana
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_divDiaSemana", timeout=25)
    time.sleep(0.6)

    dia_pt = weekday_pt_for_filter(current_date)

    # tenta com/sem acento em "Terça-Feira" (fallback)
    dia_xpath_variants = [
        f"//*[normalize-space(text())='{dia_pt}']",
    ]
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

    # Relatório Quantitativo
    safe_click(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkRelatorioQuantitativo",
        timeout=25,
    )
    time.sleep(0.6)

    # Marcar todas as turmas
    safe_click(
        driver,
        By.ID,
        "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkMarcarTurmas",
        timeout=25,
    )
    time.sleep(1)

    date_str = current_date.strftime("%d/%m/%Y")

    # Inputs de data (JS para evitar mascaras/readonly)
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

    time.sleep(0.8)

    # Exportar checkbox
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkExportar", timeout=25)
    time.sleep(0.6)

    # Select2: Tipo Exportação (Excel Sem Formatação)
    safe_click(driver, By.ID, "select2-ctl00_ctl00_ContentPlaceHolder1_cmbTipoExportacao-container", timeout=25)
    time.sleep(0.6)
    safe_click(driver, By.XPATH, "//*[normalize-space(text())='Excel Sem Formatação']", timeout=25)
    time.sleep(0.8)


def baixar_relatorio(driver, current_date: date, head_office: str) -> str:
    """
    Clica em Gerar e espera download novo do .xls. Move para target com nome único.
    """
    before = set(os.listdir(download_dir))
    safe_click(driver, By.ID, "ctl00_ctl00_ContentPlaceHolder1_btnGerar_div", timeout=30)
    time.sleep(2)

    downloaded_path = wait_for_new_download_xls(download_dir, before_files=before, timeout=180)
    target_path = move_downloaded_file_unique(downloaded_path, base_target_dir, current_date, head_office)
    return target_path


def extrair_df_relatorio(xls_file_path: str, current_date: date, head_office: str) -> pd.DataFrame:
    """
    Lê o XLS e padroniza colunas para o combinado:
      Data, Nome, Curso, Professor, Vagas, Integrantes, Trancados, Horario,
      Não Frequentes, Frequentes, Dias da Semana, Sede
    """
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
    # garante existência
    for c in selected_columns:
        if c not in data.columns:
            data[c] = ""

    out = data[selected_columns].copy()
    return out


def run_sponte_frequencia() -> str:
    if not SPONTE_EMAIL or not SPONTE_PASSWORD:
        raise RuntimeError("SPONTE_EMAIL / SPONTE_PASSWORD ausentes no .env.")

    # janela padrão (BRT)
    hoje_brt = pd.Timestamp.now(tz=TZ_NAME).date()
    start_date_range = hoje_brt - timedelta(days=START_DAYS_AGO)
    end_date_range = hoje_brt - timedelta(days=END_DAYS_AGO)

    print(f"🗓️ Janela: {start_date_range:%d/%m/%Y} -> {end_date_range:%d/%m/%Y} (BRT)")
    combined_data: list[pd.DataFrame] = []

    current_date = start_date_range
    while current_date <= end_date_range:
        # pula domingo
        if current_date.weekday() == 6:
            current_date += timedelta(days=1)
            continue

        for head_office in HEAD_OFFICES:
            success = False
            for attempt in range(1, MAX_ATTEMPTS + 1):
                user_data_dir = tempfile.mkdtemp(prefix="chrome_profile_")
                driver = None
                label = f"{current_date.strftime('%d_%m_%Y')}_{head_office}_attempt_{attempt}"

                try:
                    print(f"▶️ Processando: {current_date:%d/%m/%Y} | {head_office} | tentativa {attempt}/{MAX_ATTEMPTS}")
                    driver = build_driver(download_dir=download_dir, user_data_dir=user_data_dir)

                    # LOGIN
                    login_sponte(driver)

                    # IR PARA RELATÓRIO
                    driver.get(url_didatico)
                    wait_ready(driver, timeout=30)

                    # Evita zoom/headless inconsistências
                    try:
                        driver.execute_script("document.body.style.zoom='100%'")
                    except Exception:
                        pass

                    # Seleciona sede/empresa
                    selecionar_empresas_por_sede(driver, head_office=head_office)

                    # Configura filtros de frequência
                    configurar_filtros_frequencia(driver, current_date=current_date)

                    # Baixa relatório
                    xls_path = baixar_relatorio(driver, current_date=current_date, head_office=head_office)

                    # Extrai DF
                    df = extrair_df_relatorio(xls_path, current_date=current_date, head_office=head_office)
                    if not df.empty:
                        print(df.head(10))
                        combined_data.append(df)
                        print(f"✅ Dados adicionados ({head_office} | {current_date:%d/%m/%Y}).")
                    else:
                        print(f"ℹ️ Sem dados para adicionar ({head_office} | {current_date:%d/%m/%Y}).")

                    success = True
                    break

                except Exception as e:
                    print(f"❌ Erro ({head_office} | {current_date:%d/%m/%Y}): {e}")
                    if driver:
                        try:
                            take_debug_snapshot(driver, label=f"error_{label}")
                        except Exception:
                            pass
                    # tenta de novo
                finally:
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
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=[
            "Data","Nome","Curso","Professor","Vagas","Integrantes","Trancados",
            "Horario","Não Frequentes","Frequentes","Dias da Semana","Sede"
        ])

    # sobrescreve arquivo local
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

    if not df_100.empty:
        temp = df_100.copy()
        temp["Data"] = temp["Data_dt"].dt.strftime("%d/%m/%Y")
        temp.drop(columns=["Data_dt"], inplace=True)
        temp.to_excel(anexo_path, index=False)
    else:
        pd.DataFrame(columns=["Data", "Turma", "Curso", "Professor", "Integrantes", "Horario", "Sede"]).to_excel(
            anexo_path, index=False
        )

    enviar_relatorio_turmas_100(df_100, anexo_path)
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


def atualizar_linhas(sheet_destino, df_novos: pd.DataFrame, colunas_numericas: list[str]):
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

    # NOTE: mantém compatibilidade com seu layout (escrita inicia na linha 3)
    index_map = {(linha[idx_data], linha[idx_turma]): idx + 3 for idx, linha in enumerate(dados_existentes)}

    for _, row in df_novos.iterrows():
        row = row.fillna("")
        chave = (str(row["Data"]), str(row["Turma"]))
        valores = row.tolist()

        if chave in index_map:
            linha_idx = index_map[chave]
            cell_range = sheet_destino.range(linha_idx, 1, linha_idx, len(cabecalho))
            for i, cell in enumerate(cell_range):
                if i < len(valores):
                    coluna_nome = cabecalho[i]
                    v = valores[i]
                    if coluna_nome == "Data":
                        dt = pd.to_datetime(str(v), dayfirst=True, errors="coerce")
                        cell.value = dt.strftime("%d/%m/%Y") if pd.notna(dt) else str(v)
                    elif coluna_nome in colunas_numericas:
                        num = pd.to_numeric(str(v).replace(",", ".").strip(), errors="coerce")
                        cell.value = "" if pd.isna(num) else (int(num) if float(num).is_integer() else float(num))
                    else:
                        cell.value = str(v)
                else:
                    cell.value = ""
            sheet_destino.update_cells(cell_range)
            print(f"Atualizado: {chave}")
        else:
            valores_norm = []
            for i, col_name in enumerate(cabecalho):
                v = valores[i] if i < len(valores) else ""
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
    df = pd.read_excel(input_file)
    df.columns = [c.strip() for c in df.columns]

    # padroniza nomes para planilha
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

    colunas_numericas = ["Vagas", "Integrantes", "Trancados", "Frequente", "Não Frequentes"]
    for coluna in colunas_numericas:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    # separa online/presencial
    df_online = df[df["Turma"].astype(str).str.len().ge(3) & (df["Turma"].astype(str).str[2].str.upper() == "L")].copy()
    df_presencial = df[~(df["Turma"].astype(str).str.len().ge(3) & (df["Turma"].astype(str).str[2].str.upper() == "L"))].copy()

    # AUTH
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

    # atualiza linhas
    atualizar_linhas(sheet_presencial, df_presencial, colunas_numericas=colunas_numericas)
    atualizar_linhas(sheet_online, df_online, colunas_numericas=colunas_numericas)

    # Tipos ideais (por índice 1-based) — baseado no layout do seu sheet:
    # A:Data (DATE), B:Turma (STRING), C:Curso (STRING), D:Professor (STRING),
    # E:Vagas (NUMBER), F:Integrantes (NUMBER), G:Trancados (NUMBER), H:Horario (STRING),
    # I:Não Frequentes (NUMBER), J:Frequente (NUMBER), K:Dias da Semana (STRING), L:Sede (STRING)
    tipos_ideais = {
        1: "DATE", 2: "STRING", 3: "STRING", 4: "STRING",
        5: "NUMBER", 6: "NUMBER", 7: "NUMBER", 8: "STRING",
        9: "NUMBER", 10: "NUMBER", 11: "STRING", 12: "STRING"
    }

    # Detecta divergências por aba separadamente
    linhas_erradas_presencial = detectar_linhas_divergentes_por_sheet(service_ro, GOOGLE_SHEET_ID_FREQ, sheet_index=0, tipos_ideais=tipos_ideais)
    linhas_erradas_online = detectar_linhas_divergentes_por_sheet(service_ro, GOOGLE_SHEET_ID_FREQ, sheet_index=1, tipos_ideais=tipos_ideais)

    # Corrige tipagem nas linhas divergentes
    if linhas_erradas_presencial:
        corrigir_linhas_tipagem(
            service_rw,
            GOOGLE_SHEET_ID_FREQ,
            sheet_presencial,
            linhas_erradas_presencial,
            colunas_numericas_nomes=["Vagas", "Integrantes", "Trancados", "Não Frequentes", "Frequente"],
        )
        aplicar_formatacoes_attendance(service_rw, GOOGLE_SHEET_ID_FREQ, sheet_presencial)

    if linhas_erradas_online:
        corrigir_linhas_tipagem(
            service_rw,
            GOOGLE_SHEET_ID_FREQ,
            sheet_online,
            linhas_erradas_online,
            colunas_numericas_nomes=["Vagas", "Integrantes", "Trancados", "Não Frequentes", "Frequente"],
        )
        aplicar_formatacoes_attendance(service_rw, GOOGLE_SHEET_ID_FREQ, sheet_online)

    if not linhas_erradas_presencial and not linhas_erradas_online:
        print("✅ Nenhuma linha precisa de correção de tipagem!")

    print("✅ Sincronização Google Sheets concluída.")


# =======================
# MAIN
# =======================

def main():
    output_path = run_sponte_frequencia()
    gerar_e_enviar_100_presenca(output_path)
    google_sheets_sync_frequencia(output_path)


if __name__ == "__main__":
    main()

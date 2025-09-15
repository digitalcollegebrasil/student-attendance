import os
import time
import shutil
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime, timedelta, date
import tempfile
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

SPONTE_EMAIL = os.getenv("SPONTE_EMAIL")
SPONTE_PASSWORD = os.getenv("SPONTE_PASSWORD")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

current_dir = os.path.dirname(__file__)
DOWNLOAD_TMP = tempfile.mkdtemp(prefix="sponte_dl_")
TARGET_TMP   = tempfile.mkdtemp(prefix="sponte_target_")

download_dir     = DOWNLOAD_TMP
base_target_dir  = TARGET_TMP
COMBINED_PATH = os.path.join(current_dir, 'combined_data.xlsx')
SHEET_NAME = 'Dados'

if os.path.exists(COMBINED_PATH):
    os.remove(COMBINED_PATH)

if not os.path.exists(download_dir):
    os.makedirs(download_dir)
if not os.path.exists(base_target_dir):
    os.makedirs(base_target_dir)

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
    # garantir tipos numéricos
    for c in ["Integrantes", "Frequente", "Não Frequentes"]:
        df_base[c] = pd.to_numeric(df_base[c], errors="coerce").fillna(0)

    # parse da coluna Data (está em dd/MM/yyyy)
    df_base["Data_dt"] = pd.to_datetime(df_base["Data"], dayfirst=True, errors="coerce")

    # filtro opcional por período (últimos N dias)
    if REPORT_DAYS > 0:
        limite = pd.Timestamp.now(tz="America/Fortaleza").normalize() - pd.Timedelta(days=REPORT_DAYS)
        df_base = df_base[df_base["Data_dt"] >= limite]

    # regra: ninguém faltou e presentes == integrantes (>0)
    mask = (df_base["Não Frequentes"] == 0) & (df_base["Frequente"] == df_base["Integrantes"]) & (df_base["Integrantes"] > 0)
    df100 = df_base.loc[mask, ["Data_dt", "Turma", "Curso", "Professor", "Integrantes", "Horario", "Sede"]].copy()

    # ordenação
    df100 = df100.sort_values(["Data_dt", "Sede", "Turma"]).reset_index(drop=True)
    return df100

def remove_value_attribute(driver, element):
    driver.execute_script("arguments[0].removeAttribute('value')", element)

def set_input_value(driver, element, value):
    driver.execute_script("arguments[0].value = arguments[1]", element, value)

def get_day_of_week(date):
    return date.strftime("%A")

def move_downloaded_file(download_dir, target_dir, current_date):
    filename = f"Relatorio_{current_date.strftime('%d_%m_%Y')}.xls"
    target_path = os.path.join(target_dir, filename)
    downloaded_files = [f for f in os.listdir(download_dir) if f.endswith('.xls')]
    if downloaded_files:
        latest_file = max(downloaded_files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
        shutil.move(os.path.join(download_dir, latest_file), target_path)
        print(f"Moved XLS for {current_date.strftime('%d/%m/%Y')} to {target_path}")

def processar_turma(nome_turma):
    turmas_ignoradas = ['aulas diversas', 'aulas diversas 2', 'aulas diversas gt']
    nome_turma_normalizado = nome_turma.lower().strip()
    
    if any(turma in nome_turma_normalizado for turma in turmas_ignoradas):
        print(f"Turma ignorada: {nome_turma}")
        return None
    return nome_turma

def detectar_curso(nome_turma):
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

hoje = datetime.today()
start_date_range = hoje - timedelta(days=9)
end_date_range = hoje - timedelta(days=2)

current_date = start_date_range

def click_element(driver, element):
    driver.execute_script("arguments[0].scrollIntoView();", element)
    driver.execute_script("arguments[0].click();", element)

combined_data = []

head_offices = ["Aldeota", "Sul", "Bezerra"]

while current_date <= end_date_range:
    for head_office in head_offices:
        success = False
        while not success:
            user_data_dir = tempfile.mkdtemp()
            chrome_options = webdriver.ChromeOptions()
            prefs = {
                "download.default_directory": download_dir,
                "download.prompt_for_download": False,
                "plugins.always_open_pdf_externally": True
            }
            chrome_options.add_experimental_option("prefs", prefs)
            chrome_options.add_argument("--start-maximized")
            
            if os.getenv("GITHUB_ACTIONS") == "true":
                chrome_options.add_argument('--headless=new')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')

            chrome_options.add_argument(f"--user-data-dir={user_data_dir}")

            driver = None
            try:
                day_of_week = get_day_of_week(current_date)

                if current_date > end_date_range:
                    break

                if day_of_week == "Sunday":
                    current_date += timedelta(days=1)
                    continue

                print(f"Processing date: {current_date.strftime('%d/%m/%Y')} - {day_of_week}")

                driver = webdriver.Chrome(options=chrome_options)
                driver.get("https://www.sponteeducacional.net.br/SPRel/Didatico/Turmas.aspx")
                
                email = driver.find_element(By.ID, "txtLogin")
                email.send_keys(SPONTE_EMAIL)
                password = driver.find_element(By.ID, "txtSenha")
                password.send_keys(SPONTE_PASSWORD)

                login_button = driver.find_element(By.ID, "btnok")
                login_button.click()
                time.sleep(5)

                print(head_office)
                enterprise = driver.find_element(By.ID, "ctl00_ctl00_spnNomeEmpresa").get_attribute("innerText").strip().replace(" ", "")
                print(enterprise)
                
                combinacoes = {
                    ("Aldeota", "DIGITALCOLLEGESUL-74070"): (1, "Acessando a sede Aldeota."),
                    ("Aldeota", "DIGITALCOLLEGEBEZERRADEMENEZES-488365"): (1, "Acessando a sede Aldeota."),
                    ("Sul", "DIGITALCOLLEGEALDEOTA-72546"): (3, "Acessando a sede Sul."),
                    ("Sul", "DIGITALCOLLEGEBEZERRADEMENEZES-488365"): (3, "Acessando a sede Sul."),
                    ("Bezerra", "DIGITALCOLLEGEALDEOTA-72546"): (4, "Acessando a sede Bezerra."),
                    ("Bezerra", "DIGITALCOLLEGESUL-74070"): (4, "Acessando a sede Bezerra."),
                    ("Aldeota", "DIGITALCOLLEGEALDEOTA-72546"): (None, "O script já está na Aldeota."),
                    ("Sul", "DIGITALCOLLEGESUL-74070"): (None, "O script já está no Sul."),
                    ("Bezerra", "DIGITALCOLLEGEBEZERRADEMENEZES-488365"): (None, "O script já está na Bezerra."),
                }

                resultado = combinacoes.get((head_office, enterprise), (None, "Ação não realizada: combinação não reconhecida."))
                val, message = resultado

                print(message)

                # if val is not None:
                #     driver.execute_script(f"$('#ctl00_hdnEmpresa').val({val});javascript:__doPostBack('ctl00$lnkChange','');")
                #     time.sleep(3)

                if head_office == "Aldeota":
                    empresas_button = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_liEmpresas")
                    empresas_button.click()
                    time.sleep(2)
                    ul_element = driver.find_element(By.CSS_SELECTOR, 'ul.nav.nav-pills')
                    li_elements = ul_element.find_elements(By.TAG_NAME, 'li')
                    if li_elements:
                        first_li = li_elements[0]
                        first_li.click()
                    else:
                        print("Nenhum elemento <li> encontrado.")
                    time.sleep(2)
                elif head_office == "Sul":
                    empresas_button = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_liEmpresas")
                    empresas_button.click()
                    time.sleep(2)
                    aldeota_checkbox = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_cblEmpresas_0")
                    aldeota_checkbox.click()
                    time.sleep(3)
                    sul_checkbox = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_cblEmpresas_1")
                    sul_checkbox.click()
                    time.sleep(3)
                    ul_element = driver.find_element(By.CSS_SELECTOR, 'ul.nav.nav-pills')
                    li_elements = ul_element.find_elements(By.TAG_NAME, 'li')
                    if li_elements:
                        first_li = li_elements[0]
                        first_li.click()
                    else:
                        print("Nenhum elemento <li> encontrado.")
                    time.sleep(2)
                elif head_office == "Bezerra":
                    empresas_button = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_liEmpresas")
                    empresas_button.click()
                    time.sleep(2)
                    aldeota_checkbox = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_cblEmpresas_0")
                    aldeota_checkbox.click()
                    time.sleep(3)
                    bezerra_checkbox = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_cblEmpresas_2")
                    bezerra_checkbox.click()
                    time.sleep(3)
                    ul_element = driver.find_element(By.CSS_SELECTOR, 'ul.nav.nav-pills')
                    li_elements = ul_element.find_elements(By.TAG_NAME, 'li')
                    if li_elements:
                        first_li = li_elements[0]
                        first_li.click()
                    else:
                        print("Nenhum elemento <li> encontrado.")
                    time.sleep(2)
                
                try:
                    status_dropdown = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "select2-ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_cmbSituacaoTurma-container"))
                    )
                    click_element(driver, status_dropdown)
                except TimeoutException:
                    print("Status dropdown not clickable")
                    driver.quit()
                    continue
                time.sleep(1)

                active_status = driver.find_element(By.XPATH, "//*[text()='Vigente']")
                active_status.click()
                time.sleep(5)

                day_of_week_select = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_divDiaSemana")
                day_of_week_select.click()
                time.sleep(1)

                day_of_week_box = None
                if day_of_week == "Monday":
                    day_of_week_box = driver.find_element(By.XPATH, "//*[text()='Segunda-Feira']")
                elif day_of_week == "Tuesday":
                    day_of_week_box = driver.find_element(By.XPATH, "//*[text()='Terça-Feira']")
                elif day_of_week == "Wednesday":
                    day_of_week_box = driver.find_element(By.XPATH, "//*[text()='Quarta-Feira']")
                elif day_of_week == "Thursday":
                    day_of_week_box = driver.find_element(By.XPATH, "//*[text()='Quinta-Feira']")
                elif day_of_week == "Friday":
                    day_of_week_box = driver.find_element(By.XPATH, "//*[text()='Sexta-Feira']")
                elif day_of_week == "Saturday":
                    day_of_week_box = driver.find_element(By.XPATH, "//*[text()='Sábado']")
                elif day_of_week == "Sunday":
                    day_of_week_box = driver.find_element(By.XPATH, "//*[text()='Domingo']")
                
                if day_of_week_box:
                    day_of_week_box.click()
                time.sleep(1)

                try:
                    quantitative_report_checkbox = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkRelatorioQuantitativo"))
                    )
                    click_element(driver, quantitative_report_checkbox)
                except TimeoutException:
                    print("Quantitative report checkbox not clickable")
                    driver.quit()
                    continue
                time.sleep(1)

                try:
                    all_classes_checkbox = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkMarcarTurmas"))
                    )
                    click_element(driver, all_classes_checkbox)
                except TimeoutException:
                    print("All classes checkbox not clickable")
                    driver.quit()
                    continue
                time.sleep(3)

                start_date = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_wcdDataInicioFaltasCons_txtData")
                remove_value_attribute(driver, start_date)
                set_input_value(driver, start_date, current_date.strftime("%d/%m/%Y"))

                end_date = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_wcdDataTerminoFaltasCons_txtData")
                remove_value_attribute(driver, end_date)
                set_input_value(driver, end_date, current_date.strftime("%d/%m/%Y"))

                try:
                    export_checkbox = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkExportar"))
                    )
                    click_element(driver, export_checkbox)
                except TimeoutException:
                    print("Export checkbox not clickable")
                    driver.quit()
                    continue
                time.sleep(1)

                select2_span = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "select2-ctl00_ctl00_ContentPlaceHolder1_cmbTipoExportacao-container"))
                )
                select2_span.click()
                time.sleep(1)

                option = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//*[text()='Excel Sem Formatação']"))
                )
                option.click()
                time.sleep(1)

                try:
                    generate_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "ctl00_ctl00_ContentPlaceHolder1_btnGerar_div"))
                    )
                    click_element(driver, generate_button)
                except TimeoutException:
                    print("Generate button not clickable")
                    driver.quit()
                    continue
                time.sleep(5)

                try:
                    move_downloaded_file(download_dir, base_target_dir, current_date)
                
                    xls_file_path = os.path.join(base_target_dir, f"Relatorio_{current_date.strftime('%d_%m_%Y')}.xls")

                    data = pd.read_excel(xls_file_path, skiprows=3)

                    data['Nome'] = data['Nome'].apply(processar_turma)

                    data = data.dropna(subset=['Nome'])

                    if data.empty:
                        print(f"Nenhuma turma válida encontrada para a data {current_date.strftime('%d/%m/%Y')}.")
                        success = True
                        continue

                    nome_turma = data['Nome'].iloc[0]

                    if not nome_turma:
                        continue

                    if 'Não Frequentes' not in data.columns and 'NaoFrequente' in data.columns:
                        data['Não Frequentes'] = data['NaoFrequente']
                    if 'Frequentes' not in data.columns and 'Frequente' in data.columns:
                        data['Frequentes'] = data['Frequente']
                    if 'Dias da Semana' not in data.columns and 'DiasSemana' in data.columns:
                        data['Dias da Semana'] = data['DiasSemana']
                    if 'DataInicio' not in data.columns and 'Data Início' in data.columns:
                        data['DataInicio'] = data['Data Início']

                    data['DataInicio'] = pd.to_datetime(data['DataInicio'], dayfirst=True, errors='coerce')
                    hoje_brt = pd.Timestamp.now(tz='America/Fortaleza').date()
                    data = data[data['DataInicio'].dt.date <= hoje_brt].copy()
                    data = data.dropna(subset=['DataInicio'])

                    print(f"Nome da turma: {nome_turma}")
                    print(f"Data: {current_date.strftime('%d/%m/%Y')}")
                    print(f"Sede: {head_office}")

                    data['Data'] = current_date.strftime("%d/%m/%Y")
                    data['Curso'] = data['Nome'].apply(detectar_curso)
                    data['Sede'] = head_office

                    data['Frequentes'] = pd.to_numeric(data['Frequentes'], errors='coerce')
                    data['Não Frequentes'] = pd.to_numeric(data['Não Frequentes'], errors='coerce')

                    condicao_remover = (
                        ((data['Frequentes'] == 0) & (data['Não Frequentes'] == 0)) |
                        ((data['Frequentes'] == 0) & (data['Não Frequentes'].isin([1, 2]))) |
                        ((data['Não Frequentes'] == 0) & (data['Frequentes'].isin([1, 2])))
                    )

                    data = data[~condicao_remover]

                    selected_columns = [
                        'Data', 'Nome', 'Curso', 'Professor', 'Vagas', 'Integrantes',
                        'Trancados', 'Horario', 'Não Frequentes', 'Frequentes', 'Dias da Semana', 'Sede'
                    ]

                    selected_columns_df = data[selected_columns]
                    print(selected_columns_df)
                    print(f"Dados: {data}")
                    combined_data.append(selected_columns_df)
                    print(f"Dados do dia {current_date.strftime('%d/%m/%Y')} adicionados com sucesso.")
                    success = True
                except Exception as e:
                    print(f"Erro ao processar a data {current_date.strftime('%d/%m/%Y')}: {str(e)}")
                finally:
                    try:
                        driver.quit()
                    except:
                        pass
            except Exception as e:
                print(f"Erro ao processar a data {current_date.strftime('%d/%m/%Y')}: {str(e)}")
                driver.quit()
    current_date += timedelta(days=1)

if combined_data:
    final_df = pd.concat(combined_data)
    final_output_path = os.path.join(current_dir, 'combined_data.xlsx')
    final_df.to_excel(final_output_path, index=False)
    print(f"Combined data saved to {final_output_path}  ")
else:
    pd.DataFrame(columns=[
        'Data','Nome','Curso','Professor','Vagas','Integrantes','Trancados',
        'Horario','Não Frequentes','Frequentes','Dias da Semana','Sede'
    ]).to_excel(COMBINED_PATH, index=False)

print("Download process completed.")

input_file = 'combined_data.xlsx'
df = pd.read_excel(input_file)

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
            sheet_destino.append_row(valores, value_input_option='USER_ENTERED')
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
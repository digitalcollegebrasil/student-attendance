import os
import time
import shutil
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime, timedelta
import tempfile
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

email_address = os.getenv("SPONTE_EMAIL")
password_value = os.getenv("SPONTE_PASSWORD")
credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")

current_dir = os.path.dirname(__file__)
download_dir = os.path.join(current_dir, 'downloads')
base_target_dir = os.path.join(current_dir, 'target')

if not os.path.exists(download_dir):
    os.makedirs(download_dir)
if not os.path.exists(base_target_dir):
    os.makedirs(base_target_dir)

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

download_dir = "/tmp"
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
                email.send_keys(email_address)
                password = driver.find_element(By.ID, "txtSenha")
                password.send_keys(password_value)

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

                    print(f"Nome da turma: {nome_turma}")
                    print(f"Data: {current_date.strftime('%d/%m/%Y')}")
                    print(f"Sede: {head_office}")

                    data['Data'] = current_date.strftime("%d/%m/%Y")
                    data['Curso'] = data['Nome'].apply(detectar_curso)
                    data['Sede'] = head_office

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
                    current_date += timedelta(days=1)
                    try:
                        driver.quit()
                    except:
                        pass
            except Exception as e:
                print(f"Erro ao processar a data {current_date.strftime('%d/%m/%Y')}: {str(e)}")
                driver.quit()

print("Download process completed.")

if combined_data:
    final_df = pd.concat(combined_data)
    final_output_path = os.path.join(current_dir, 'combined_data.xlsx')
    final_df.to_excel(final_output_path, index=False)
    print(f"Combined data saved to {final_output_path}")
else:
    print("No data to save.")
    exit()

input_file = 'combined_data.xlsx'
df = pd.read_excel(input_file)

df.rename(columns={
    "Nome": "Turma",
    "Frequentes": "Frequente",
}, inplace=True)

if 'Turma' not in df.columns or 'Data' not in df.columns:
    print("Colunas 'Turma' e 'Data' são necessárias.")
    exit()

df_online = df[df['Turma'].astype(str).str[2].str.upper() == 'L']
df_presencial = df[df['Turma'].astype(str).str[2].str.upper() != 'L']

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json, scope)
client = gspread.authorize(creds)

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
                    cell.value = str(valores[i])
                else:
                    cell.value = ''
            sheet_destino.update_cells(cell_range)
            print(f"Atualizado: {chave}")
        else:
            sheet_destino.append_row(valores)
            print(f"Inserido: {chave}")

        time.sleep(1)

atualizar_linhas(sheet_presencial, df_presencial)
atualizar_linhas(sheet_online, df_online)

print("Dados atualizados com sucesso.")
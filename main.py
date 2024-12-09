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

load_dotenv()

head_office = os.getenv("HEAD_OFFICE")
email_address = os.getenv("SPONTE_EMAIL")
password_value = os.getenv("SPONTE_PASSWORD")

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

chrome_options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "plugins.always_open_pdf_externally": True
}
chrome_options.add_experimental_option("prefs", prefs)

start_date_range = datetime.strptime("01/09/2024", "%d/%m/%Y")
end_date_range = datetime.strptime("07/12/2024", "%d/%m/%Y")

current_date = start_date_range

def click_element(driver, element):
    driver.execute_script("arguments[0].scrollIntoView();", element)
    driver.execute_script("arguments[0].click();", element)

combined_data = []

while current_date <= end_date_range:
    success = False
    while not success:
        try:
            day_of_week = get_day_of_week(current_date)

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
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".select2-selection__rendered"))
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

                column_mapping = {
                    'Nome': 'Nome',
                    'Professor': 'Professor',
                    'Vagas': 'Vagas',
                    'Integrantes': 'Integrantes',
                    'Trancados': 'Trancados',
                    'Horário': 'Horario',
                    'Não Frequentes': 'NaoFrequente',
                    'Frequentes': 'Frequente',
                    'Dias da Semana': 'DiasSemana'
                }

                selected_columns = {}
                for desired, real in column_mapping.items():
                    if real in data.columns:
                        selected_columns[desired] = data[real]

                selected_columns_df = pd.DataFrame(selected_columns)
                selected_columns_df['Data'] = current_date.strftime("%d/%m/%Y")
                selected_columns_df['Sede'] = head_office

                combined_data.append(selected_columns_df)
            except Exception as e:
                print(f"Erro ao processar a data {current_date.strftime('%d/%m/%Y')}: {str(e)}")
            finally:
                current_date += timedelta(days=1)

            driver.quit()

            success = True
        except Exception as e:
            print(f"Erro ao processar a data {current_date.strftime('%d/%m/%Y')}: {str(e)}")
            driver.quit()

print("Download process completed.")

final_df = pd.concat(combined_data)
final_output_path = os.path.join(current_dir, 'combined_data.xlsx')
final_df.to_excel(final_output_path, index=False)
print(f"Combined data saved to {final_output_path}")

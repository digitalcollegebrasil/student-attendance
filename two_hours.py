import os
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime, timedelta
import time
import shutil

load_dotenv()

head_office = os.getenv("HEAD_OFFICE")
email_address = os.getenv("SPONTE_EMAIL")
password_value = os.getenv("SPONTE_PASSWORD")

current_dir = os.path.dirname(__file__)

download_dir = current_dir
base_target_dir = current_dir

def remove_value_attribute(driver, element):
    driver.execute_script("arguments[0].removeAttribute('value')", element)

def set_input_value(driver, element, value):
    driver.execute_script("arguments[0].value = arguments[1]", element, value)

def get_day_of_week(date):
    day_of_week = date.strftime("%A")
    return day_of_week

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

start_date_range = datetime.strptime("07/02/2024", "%d/%m/%Y")
end_date_range = datetime.strptime("20/05/2024", "%d/%m/%Y")

current_date = start_date_range

while current_date <= end_date_range:
    day_of_week = get_day_of_week(current_date)

    if day_of_week in ["Friday", "Saturday", "Sunday"]:
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

    enterprise = driver.find_element(By.ID, "ctl00_ctl00_spnNomeEmpresa").get_attribute("innerText").strip().replace(" ", "")

    if head_office == "Aldeota":
        if enterprise == "DIGITALCOLLEGESUL-74070":
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
    elif head_office == "Sul":
        if enterprise == "DIGITALCOLLEGEALDEOTA-72546":
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
    
    status_dropdown = driver.find_element(By.ID, "select2-ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_cmbSituacaoTurma-container")
    status_dropdown.click()

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
        quantitative_report_checkbox = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_chkRelatorioQuantitativo")
        quantitative_report_checkbox.click()
    except TimeoutException:
        print("Quantitative report checkbox not clickable")
        driver.quit()
        continue
    time.sleep(1)

    try:
        number_of_faults = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_txtNroFaltasConsecutivas")
        remove_value_attribute(driver, number_of_faults)
        set_input_value(driver, number_of_faults, "2")
    except TimeoutException:
        print("Number of faults not clickable")
        driver.quit()
        continue
    time.sleep(1)

    try:
        fsl_01 = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_cblTurmas_30")
        fsl_01.click()
    except TimeoutException:
        print("FSL 01 not clickable")
        driver.quit()
        continue
    time.sleep(1)

    try:
        fsl_02 = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_cblTurmas_31")
        fsl_02.click()
    except TimeoutException:
        print("FSL 02 not clickable")
        driver.quit()
        continue
    time.sleep(1)

    try:
        fsl_03 = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_cblTurmas_32")
        fsl_03.click()
    except TimeoutException:
        print("FSL 03 not clickable")
        driver.quit()
        continue
    time.sleep(1)

    start_date = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_wcdDataInicioFaltasCons_txtData")
    remove_value_attribute(driver, start_date)
    set_input_value(driver, start_date, current_date.strftime("%d/%m/%Y"))

    end_date = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_ContentPlaceHolder2_tab_tabTurmasRegulares_wcdDataTerminoFaltasCons_txtData")
    remove_value_attribute(driver, end_date)
    set_input_value(driver, end_date, current_date.strftime("%d/%m/%Y"))

    try:
        export_checkbox = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_chkExportar")
        export_checkbox.click()
    except TimeoutException:
        print("Export checkbox not clickable")
        driver.quit()
        continue
    time.sleep(1)

    try:
        export_select = driver.find_element(By.ID, "select2-ctl00_ctl00_ContentPlaceHolder1_cmbTipoExportacao-container")
        export_select.click()
    except TimeoutException:
        print("Export select not clickable")
        driver.quit()
        continue
    time.sleep(1)

    try:
        export_option = driver.find_element(By.XPATH, "//*[text()='Excel Sem Formatação']")
        export_option.click()
    except TimeoutException:
        print("Export option not clickable")
        driver.quit()
        continue
    time.sleep(1)

    try:
        generate_button = driver.find_element(By.ID, "ctl00_ctl00_ContentPlaceHolder1_btnGerar_div")
        generate_button.click()
        time.sleep(8)
        print(f"Downloaded XLS for {current_date.strftime('%d/%m/%Y')}")

        target_dir = os.path.join(base_target_dir, current_date.strftime('%Y-%m-%d'))
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        move_downloaded_file(download_dir, target_dir, current_date)

        driver.close()
    except TimeoutException:
        print("Generate button not clickable")
        driver.quit()
        continue
    
    time.sleep(3)
    driver.quit()

    current_date += timedelta(days=1)

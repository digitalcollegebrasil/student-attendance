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

start_date_range = datetime.strptime("29/06/2024", "%d/%m/%Y")
end_date_range = datetime.strptime("30/06/2024", "%d/%m/%Y")

current_date = start_date_range

def click_element(driver, element):
    driver.execute_script("arguments[0].scrollIntoView();", element)
    driver.execute_script("arguments[0].click();", element)

while current_date <= end_date_range:
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

    move_downloaded_file(download_dir, base_target_dir, current_date)

    driver.quit()

    current_date += timedelta(days=1)

print("Download process completed.")

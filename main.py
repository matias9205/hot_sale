import io
import json
import os
import random
import time
import pandas as pd
from datetime import datetime
import requests
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import urllib.parse
from dotenv import load_dotenv

from db import create_sql_connection

load_dotenv()

options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("user-agent=Mozilla/5.0")
driver = webdriver.Chrome(options=options)

session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    raise_on_status=False
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("http://", adapter)
session.mount("https://", adapter)

SQL_SERVER_USER = os.getenv("SQL_SERVER_USER")
SQL_SERVER_PASS = os.getenv("SQL_SERVER_PASS")
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST")
SQL_SERVER_DB = os.getenv("SQL_SERVER_DB")

DB_URL = f"mssql+pyodbc://{SQL_SERVER_USER}:{SQL_SERVER_PASS}@{SQL_SERVER_HOST}/{SQL_SERVER_DB}?driver=ODBC+Driver+17+for+SQL+Server"

BASE_URL = "https://listado.mercadolibre.com.ar/"

df_links = pd.read_csv("./input_links.csv")
print(df_links)

def get_links(_input_links_ = pd.DataFrame):
    products_links = []
    for index, row in _input_links_.iterrows():
        url__ = BASE_URL+row['link']
        i = 1
        url = None
        while True:
            if i == 1:
                url = f"{url__}_NoIndex_True_original*category*landing_true?original_category_landing=true"
            else:
                url = f"{url__}_Desde_{i}_NoIndex_True_original*category*landing_true?original_category_landing=true"
            url_safe = urllib.parse.quote(url, safe=":/?=&")
            print(url_safe)
            try:
                res = requests.get(url_safe)
                if res.status_code != 200 or i > 51:
                    print("Stopping loop due to invalid response or max iteration reached.")
                    break
                driver.get(url_safe)
                time.sleep(random.uniform(2, 5))
                # links = driver.find_elements(By.XPATH, '//div[@class="poly-card__content"]/h3[@class="poly-component__title-wrapper"]/a')
                # print(f"--------------------LINKS COUNT: {len(links)}----------------------------")
                # for link in links:
                #     products_links.append({
                #         "name": link.text,
                #         "link": link.get_attribute('href')
                #     })
                try:
                    h3_titles = driver.find_elements(By.XPATH, '//h3//a')
                    print(f"--------------------LINKS COUNT: {len(h3_titles)}----------------------------")
                    for link in h3_titles:
                        products_links.append({
                            "name": link.text,
                            "link": link.get_attribute('href')
                        })      
                except:
                    print("❌ No hay <a> dentro del <h3>")          
            except Exception as e:
                print(f"There was an error: {str(e)}, fetching data from the link: {url}")
            i += 50
    print(products_links)
    return products_links

def safe_find_text(by, path, multiple=False, index=0, default="Unknown"):
    try:
        if multiple:
            elements = driver.find_elements(by, path)
            return elements[index].text if elements and len(elements) > index else default
        else:
            element = driver.find_element(by, path)
            return element.text if element else default
    except Exception:
        return default
    
errors_extracting = []
    
def extract_data(url):
    product_data = {
        "main_category": "",
        "category": "",
        "sub_category": "",
        "brand": "",
        "condition": "",
        "specs": {},
        "total_solds": "",
        "recommendation": "",
        "title": "",
        "rating": "",
        "original_price": "",
        "price_with_discount": "",
        "discount_aplicated": "",
        "total_califications": "",
        "quality_price_relation": "",
        "url": url
    }
    try:
        driver.get(url)
        time.sleep(random.uniform(2, 5))
        categories = driver.find_elements(By.XPATH, '//*[@id="breadcrumb"]')
        if categories:
            main_category = driver.find_elements(By.XPATH, '//*[@id=":R5769gm:"]/ol/li[1]/a')
            product_data["main_category"] = main_category[0].text if main_category else "Unknown"
            category = driver.find_elements(By.XPATH, '//*[@id=":R5769gm:"]/ol/li[2]/a')
            product_data["category"] = category[0].text if category else "Unknown"
            sub_category = driver.find_elements(By.XPATH, '//*[@id=":R5769gm:"]/ol/li[3]/a')
            product_data["sub_category"] = sub_category[0].text if sub_category else "Unknown"
        else:
            product_data["main_category"] = "Unknown"
            product_data["category"] = "Unknown"
            product_data["sub_category"] = "Unknown"
        brand_text = safe_find_text(By.XPATH, '//*[@id="ui-pdp-main-container"]/div[1]/div/div[2]/div[2]/a/div/p')
        if brand_text:
            product_data["brand"] = brand_text.split(" ")[-1]
        else:
            print(f"⚠️ Brand not found for {url}")
            product_data["brand"] = "Unknown"
        condition_and_sales = safe_find_text(By.XPATH, '//*[@id="ui-pdp-main-container"]/div[1]/div/div[2]/div[2]/div[1]/div/div[1]/span')
        if condition_and_sales and "|" in condition_and_sales:
            parts = [p.strip() for p in condition_and_sales.split("|")]
            product_data["condition"] = parts[0] if len(parts) > 0 else "Unknown"
            product_data["total_solds"] = parts[1] if len(parts) > 1 else "Unknown"
        product_recommended = safe_find_text(By.XPATH, '//*[@id="ui-pdp-main-container"]/div[1]/div/div[2]/div[2]/div[1]/div/div[2]/div/div/div/div/div/span')
        print(f"----------------------------------PRODUCT RECOMMENDED: {product_recommended}----------------------------------")
        product_more_sold = safe_find_text(By.XPATH, '//*[@id="ui-pdp-main-container"]/div[1]/div/div[2]/div[2]/div[1]/div/div[2]/div/div[1]/div/a')
        print(f"----------------------------------PRODUCT MORE SOLD: {product_more_sold}----------------------------------")
        product_data["recommendation"] = (
            product_more_sold if product_more_sold
            else product_recommended if product_recommended
            else "Unknown"
        )
        product_data["title"] = safe_find_text(By.XPATH, '//*[@id="ui-pdp-main-container"]/div[1]/div/div[2]/div[2]/div[1]/div/div[2]/h1')
        product_data["rating"] = safe_find_text(By.XPATH, '//*[@id="ui-pdp-main-container"]/div[1]/div/div[2]/div[2]/div[1]/div/div[3]/a/span[1]')
        product_data["original_price"] = safe_find_text(By.XPATH, '/html/body/main/div[2]/div[5]/div[2]/div[1]/div/div[2]/div[2]/div[2]/div/div[1]/span/s/span[2]')
        product_data["price_with_discount"] = safe_find_text(By.XPATH, '/html/body/main/div[2]/div[5]/div[2]/div[1]/div/div[2]/div[2]/div[2]/div/div[1]/div[1]/span[1]/span/span[2]')
        product_data["discount_aplicated"] = safe_find_text(By.XPATH, '/html/body/main/div[2]/div[5]/div[2]/div[1]/div/div[2]/div[2]/div[2]/div/div[1]/div[1]/span[2]/span')
        product_data["total_califications"] = safe_find_text(By.XPATH, '//*[@id="reviews_capability_v3"]/div/section/div/div[1]/div[1]/div[1]/div[2]/div[2]/p')
        product_data["quality_price_relation"] = safe_find_text(By.XPATH, '//*[@id="reviews_capability_v3"]/div/section/div/div[1]/div[2]/table/tbody/tr[1]/td[2]/div/p')
        if product_data['category'] == 'Notebooks y Accesorios':
            if not product_data['specs']:
                product_data['specs'] = {}
            cpu = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[1]')
            product_data['specs']['CPU'] = cpu.split(": ")[1] if cpu else "Unknown"
            operating_system = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[2]')
            product_data['specs']['operating_system'] = f"""Windows {operating_system.split(": ")[1]}""" if operating_system else "Unknown"
            storage = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[5]')
            product_data['specs']['storage_type'] = "SSD" if "SSD" in storage else "Unknown"
            product_data['specs']['storage_amount'] = storage.split(": ")[1] if storage else "Unknown"
            ram_memory = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[6]')
            product_data['specs']['ram_memory'] = ram_memory.split(": ")[1] if ram_memory else "Unknown"
            gpu = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[7]')
            product_data['specs']['GPU'] = gpu.split(": ")[1] if gpu else "Unknown"
            tactil_display = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[8]')
            product_data['specs']['tactil'] = tactil_display.split(": ")[1] if tactil_display else "Unknown"
            display_inches = safe_find_text(By.XPATH, '//*[@id=":R57e9bil99gm:-value"]')
            product_data['specs']['display_inches'] = display_inches if display_inches else "Unknown"
        elif product_data['category'] == 'Celulares y Smartphones':
            if not product_data['specs']:
                product_data['specs'] = {}            
            display_inches = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[2]')
            product_data['specs']['display_inches'] = display_inches if display_inches else "Unknown"
            back_cameras = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[3]')
            product_data['specs']['back_cameras'] = back_cameras if back_cameras else "Unknown"
            front_cameras = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[4]')
            product_data['specs']['front_cameras'] = front_cameras if front_cameras else "Unknown"
            cpu = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[5]')
            product_data['specs']['CPU'] = cpu if cpu else "Unknown"
            storage = safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[7]')
            product_data['specs']['storage'] = storage if storage else "Unknown"
        print(product_data)
        df = pd.json_normalize(product_data, max_level=0)
        return df
    except Exception as e:
        print(f"❌ Error: {str(e)}\nURL: {url}")
        errors_extracting.append({'url': url, 'error': str(e)})
        return pd.DataFrame([{}])
    
def load_data(db_url_, df_:pd.DataFrame):
    print(f"-----------------------------------DB_URL: {db_url_}-----------------------------------")
    table_name = "hot_sale_products"
    try:
        engine = create_sql_connection(DB_URL)
        df_.to_sql(table_name, con=engine, if_exists="replace", index=False)
        print(f"TABLE {table_name} WAS SAVED SUCCESSFULLY")
    except Exception as e:
        print(f"The was an error: {str(e)} while saving table {table_name} in database")

if __name__ == "__main__":
    product_links = get_links(df_links)
    all_products = pd.DataFrame()
    for prod in product_links:
        print("------------------------------------------------------------PROD------------------------------------------------------------")
        print(prod)
        df_product = extract_data(prod['link'])
        print(df_product.columns)
        if not df_product.empty:
            all_products = pd.concat([all_products, df_product], ignore_index=True)
    df_error_extracting = pd.DataFrame(errors_extracting)
    print(all_products.info())
    all_products.to_csv('mercado_libre_hot_sale_2025.csv', sep=";", index=False)
    load_data(DB_URL, all_products)
    df_error_extracting.to_csv('errors_extracting_data.csv', sep=";", index=False)
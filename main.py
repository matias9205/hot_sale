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
from dotenv import load_dotenv

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

BASE_URL = "https://listado.mercadolibre.com.ar/"

def get_links(base_url_):
    i = 1
    products_links = []
    while True:
        path = f"_Desde_{i}_Container_mk-t1-hot-sale-2025-12-y-18-cuotas_NoIndex_True"
        url = base_url_ + path
        print(url)
        try:
            res = requests.get(url)
            if res.status_code != 200 or i > 48:
                print("Stopping loop due to invalid response or max iteration reached.")
                break
            driver.get(url)
            time.sleep(random.uniform(2, 5))
            links_divs = driver.find_elements(By.CLASS_NAME, "poly-card__content")
            for div in links_divs:
                product_title = div.find_element(By.CLASS_NAME, "poly-component__title-wrapper")
                link_product = product_title.find_element(By.TAG_NAME, "a")
                products_links.append({
                    "name": product_title.text,
                    "link": link_product.get_attribute('href')
                })
        except Exception as e:
            print(f"There was an error: {str(e)}, fetching data from the link: {url}")
        i += 48
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
    
def extract_data(url):
    product_data = {
        "main_category": "",
        "category": "",
        "sub_category": "",
        "brand": "",
        "condition": "",
        "total_solds": "",
        "recommendation": "",
        "title": "",
        "rating": "",
        "original_price": "",
        "price_with_discount": "",
        "discount_aplicated": "",
        "total_califications": "",
        "quality_price_relation": ""
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
        print(product_data)
        # columns_order = [
        #     'title', 
        #     'brand', 
        #     'main_category', 
        #     'category', 
        #     'sub_category',
        #     'condition',
        #     'original_price',
        #     'price_with_discount', 
        #     'discount_aplicated',
        #     'rating', 
        #     'total_califications', 
        #     'total_solds',
        #     'recommendation', 
        #     'quality_price_relation'
        # ]
        df = pd.json_normalize(product_data)
        # df = df.reindex(columns_order)
        return df
    except Exception as e:
        print(f"❌ Error: {str(e)}\nURL: {url}")
        return pd.DataFrame([{}])

if __name__ == "__main__":
    product_links = get_links(BASE_URL)
    all_products = pd.DataFrame()
    for prod in product_links:
        print("------------------------------------------------------------PROD------------------------------------------------------------")
        print(prod)
        df_product = extract_data(prod['link'])
        print(df_product.columns)
        all_products = pd.concat([all_products, df_product], ignore_index=True)
    # columns_order = [
    #     'title', 
    #     'brand', 
    #     'main_category', 
    #     'category', 
    #     'sub_category',
    #     'condition',
    #     'original_price',
    #     'price_with_discount', 
    #     'discount_aplicated',
    #     'rating', 
    #     'total_califications', 
    #     'total_solds',
    #     'recommendation', 
    #     'quality_price_relation'
    # ]
    # all_products = all_products.reindex(columns_order)
    print(all_products.info())
    all_products.to_csv('mercado_libre_hot_sale_2025.csv', sep=";", index=False)
import random
import time
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import logging
import sys

page_ = int(float(sys.argv[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"  # Formato de fecha
)

BASE_URL = "https://www.mercadolibre.com.ar"

client = MongoClient("mongodb://localhost:27017/")
db = client['ETL_Mercado_Libre']

class Scrapper:
    def __init__(self, url_, client_, db_):
        self.url = url_
        self.options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome(options=self.options)
        self.options.add_argument("--headless")
        self.options.add_argument("user-agent=Mozilla/5.0")
        self.options.add_argument(f'--proxy-server=http://IP:{random.randint(3000, 8000)}')
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.client: MongoClient = client_
        self.db = client[db_]
        self.products_links = []
        self.errors_extracting = []
    
    def get_links(self) -> list:
        i=1
        product_link = {}
        while True:
            url_path = f"/ofertas?page={i}"
            complete_url = self.url+url_path
            logging.info(complete_url)
            try:
                res = requests.get(complete_url)
                if res.status_code != 200 or i > page_:
                    logging.error("Stopping loop due to invalid response or max iteration reached.")
                    break
                self.driver.get(complete_url)
                time.sleep(random.uniform(2, 5))
                try:
                    h3_titles = self.driver.find_elements(By.XPATH, '//h3//a')
                    brands = self.driver.find_elements(By.XPATH, '//span[contains(@class, "brand")]')
                    sellers = self.driver.find_elements(By.XPATH, '//span[contains(@class, "seller")]')
                    logging.info(f"--------------------LINKS COUNT: {len(h3_titles)}----------------------------")
                    for idx, link in enumerate(h3_titles):
                        brand_text = brands[idx].text if idx < len(brands) else ""
                        seller_text = sellers[idx].text if idx < len(sellers) else ""
                        self.products_links.append({
                            "name": link.text,
                            "link": link.get_attribute('href'),
                            "brand": brand_text,
                            "seller": seller_text
                        })
                except:
                    logging.error("No hay <a> dentro del <h3>") 
            except Exception as e:
                logging.error(f"There was an error: {str(e)}, fetching data from the link: {complete_url}")
            i += 1
        logging.info(self.products_links)
        return self.products_links
    
    def safe_find_text(self, by, path, multiple=False, index=0, default=""):
        try:
            if multiple:
                elements = self.driver.find_elements(by, path)
                return elements[index].text if elements and len(elements) > index else default
            else:
                element = self.driver.find_element(by, path)
                return element.text if element else default
        except Exception:
            return default

    def fetch_data_from_link(self, link__: str, name__: str, brand_: str, seller_: str) -> dict:
        logging.info("-------------------------------------------------------------------------------------------------------------")
        logging.info("-------------------------------------------------------------------------------------------------------------")
        logging.info(link__)
        product_data = {
            "main_category": "",
            "category": "",
            "sub_category": "",
            "brand": brand_,
            "condition": "",
            "specs": {},
            "total_solds": "",
            "recommendation": "",
            "title": name__,
            "rating": "",
            "original_price": "",
            "price_with_discount": "",
            "discount_aplicated": "",
            "total_califications": "",
            "quality_price_relation": "",
            "url": link__,
            "stock": "",
            "delivery_time": "",
            "delivery_cost": "",
            "payment_method": "",
            "seller": seller_,
            "warranty": ""
        }
        try:
            self.driver.get(link__)
            time.sleep(random.uniform(2, 5))
            """
            EXTRAEMOS CATEGORIA PRINCIPAL, CATEGORIAS, SUBCATEGORIAS
            """
            categories = self.driver.find_element(By.TAG_NAME, "nav")
            if categories:
                main_category = categories.find_elements(By.XPATH, '//*/ol/li[1]/a')
                product_data["main_category"] = main_category[0].text if main_category else "Unknown"
                category = categories.find_elements(By.XPATH, '//*/ol/li[2]/a')
                product_data["category"] = category[0].text if category else "Unknown"
                sub_category = categories.find_elements(By.XPATH, '//*/ol/li[3]/a')
                product_data["sub_category"] = sub_category[0].text if sub_category else "Unknown"
            else:
                product_data["main_category"] = "Unknown"
                product_data["category"] = "Unknown"
                product_data["sub_category"] = "Unknown"

            """
            EXTRAEMOS LA CONDICION Y EL TOTAL DE VENDIDOS
            """
            condition_and_sales = self.safe_find_text(By.XPATH, '//*[@id="ui-pdp-main-container"]/div[1]/div/div[2]/div[2]/div[1]/div/div[1]/span')
            if condition_and_sales and "|" in condition_and_sales:
                parts = [p.strip() for p in condition_and_sales.split("|")]
                product_data["condition"] = parts[0] if len(parts) > 0 else ""
                product_data["total_solds"] = parts[1] if len(parts) > 1 else ""

            """
            EXTRAEMOS QUE RECOMENDACION TIENE EL PRODUCTO
            """
            div_recomendation = self.driver.find_element(By.XPATH, '//*[@id="ui-pdp-main-container"]/div[1]/div')        
            try:
                product_recommended = div_recomendation.find_element(By.XPATH, './div[2]/div[2]/div[1]/div/div[2]/div/div/div/div/div/span')
                print(f"PRODUCT RECOMMENDED: {product_recommended.text}")
            except NoSuchElementException:
                product_recommended = None
                print("PRODUCT RECOMMENDED: Not Found")

            try:
                product_more_sold = div_recomendation.find_element(By.XPATH, './div[2]/div[2]/div[1]/div/div[2]/div/div[1]/div/a')
                print(f"PRODUCT MORE SOLD: {product_more_sold.text}")
            except NoSuchElementException:
                product_more_sold = None
                print("PRODUCT MORE SOLD: Not Found")

            try:
                product_current_offer = div_recomendation.find_element(By.XPATH, './div[1]/div/div[2]/div')
                print(f"PRODUCT CURRENT OFFER: {product_current_offer.text}")
            except NoSuchElementException:
                product_current_offer = None
                print("PRODUCT CURRENT OFFER: Not Found")

            product_data["recommendation"] = (
                product_more_sold.text if product_more_sold else
                product_recommended.text if product_recommended else
                product_current_offer.text if product_current_offer and 'OFERTA DEL DÍA' in product_current_offer.text else
                ""
            )

            product_data["rating"] = self.safe_find_text(By.XPATH, '//*[@id="ui-pdp-main-container"]/div[1]/div/div[2]/div[2]/div[1]/div/div[3]/a/span[1]')
            product_data["original_price"] = self.safe_find_text(By.XPATH, '//*[@id="price"]/div/div[1]/span/s/span[2]')
            product_data["price_with_discount"] = self.safe_find_text(By.XPATH, '//*[@id="price"]/div/div[1]/div[1]/span[1]/span/span[2]')
            product_data["discount_aplicated"] = self.safe_find_text(By.XPATH, '//*[@id="price"]/div/div[1]/div[1]/span[2]/span')
            product_data["total_califications"] = self.safe_find_text(By.XPATH, '//*[@id="reviews_capability_v3"]/div/section/div/div[1]/div[1]/div[1]/div[2]/div[2]/p')
            product_data["quality_price_relation"] = self.safe_find_text(By.XPATH, '//*[@id="reviews_capability_v3"]/div/section/div/div[1]/div[2]/table/tbody/tr[1]/td[2]/div/p')
            product_data["stock"] = self.safe_find_text(By.XPATH, '//*[@id="quantity-selector"]/span/span[4]')
            product_data['delivery_time'] = self.safe_find_text(By.XPATH, '//*[@id=":Rad4p99gm:"]/li[2]/div/div[3]/div[1]/div/div/p[1]/span')
            if product_data['delivery_time'] == "Llega gratis hoy":
                product_data['delivery_cost'] = 0
            payment_method = self.safe_find_text(By.XPATH, '//*[@id="pricing_price_subtitle"]')
            if 'Mismo precio en' in payment_method and 'cuotas' in payment_method:
                product_data['payment_method'] = "Cuotas sin interes"
            else:
                product_data['payment_method'] = "Other"
            product_data['warranty'] = self.safe_find_text(By.XPATH, '//*[@id="buybox-form"]/ul/li[3]/div/div/div')
            if product_data['category'] == 'Notebooks y Accesorios':
                if not product_data['specs']:
                    product_data['specs'] = {}
                cpu = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[1]')
                product_data['specs']['CPU'] = cpu.split(": ")[1] if cpu else "Unknown"
                operating_system = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[2]')
                product_data['specs']['operating_system'] = f"""Windows {operating_system.split(": ")[1]}""" if operating_system else "Unknown"
                storage = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[5]')
                product_data['specs']['storage_type'] = "SSD" if "SSD" in storage else "Unknown"
                product_data['specs']['storage_amount'] = storage.split(": ")[1] if storage else "Unknown"
                ram_memory = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[6]')
                product_data['specs']['ram_memory'] = ram_memory.split(": ")[1] if ram_memory else "Unknown"
                gpu = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[7]')
                product_data['specs']['GPU'] = gpu.split(": ")[1] if gpu else "Unknown"
                tactil_display = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[8]')
                product_data['specs']['tactil'] = tactil_display.split(": ")[1] if tactil_display else "Unknown"
                display_inches = self.safe_find_text(By.XPATH, '//*[@id=":R57e9bil99gm:-value"]')
                product_data['specs']['display_inches'] = display_inches if display_inches else "Unknown"
            elif product_data['category'] == 'Celulares y Smartphones':
                if not product_data['specs']:
                    product_data['specs'] = {}            
                display_inches = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[2]')
                product_data['specs']['display_inches'] = display_inches if display_inches else "Unknown"
                back_cameras = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[3]')
                product_data['specs']['back_cameras'] = back_cameras if back_cameras else "Unknown"
                front_cameras = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[4]')
                product_data['specs']['front_cameras'] = front_cameras if front_cameras else "Unknown"
                cpu = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[5]')
                product_data['specs']['CPU'] = cpu if cpu else "Unknown"
                storage = self.safe_find_text(By.XPATH, '//*[@id="ui-vpp-highlighted-specs"]/div[2]/div/ul/li[7]')
                product_data['specs']['storage'] = storage if storage else "Unknown"
                
            dict_order = [
                "title",
                "brand",
                "url",
                "condition",
                "main_category",
                "category",
                "sub_category",
                "original_price",
                "price_with_discount",
                "discount_aplicated",
                "total_solds",
                "recommendation",
                "rating",
                "total_califications",
                "quality_price_relation",
                "stock",
                "delivery_time",
                "delivery_cost",
                "payment_method",
                "seller",
                "warranty",
                "specs"
            ]
            product_data_ordered = {k: product_data[k] for k in dict_order}
            logging.info({'name': name__, 'product_info': product_data_ordered})
            logging.info("-------------------------------------------------------------------------------------------------------------")
            logging.info("-------------------------------------------------------------------------------------------------------------")
            return product_data_ordered
        except Exception as e:
            logging.error(f"Error: {str(e)}\nNAME: {name__}\nURL: {link__}")
            self.errors_extracting.append({'name': name__, 'url': link__, 'error': str(e)})
            return {}

    def normalize_price(self, p):
        if p is None:
            return ""
        return str(p).replace(".", "").replace(",", "").strip()

    def insert_data(self, dict_: dict, collection_: str, find_field: str, field_: str, payload_: dict):
        collection = self.db[collection_]
        filtro = {find_field: dict_[field_]}
        if collection_ == "products_history":
            try:
                last_entry = collection.find_one(
                    filtro,
                    sort=[("extracted_at", -1)]
                )
                current_price = self.normalize_price(payload_.get("original_price"))
                previous_price = self.normalize_price(last_entry.get("original_price")) if last_entry else None
                if last_entry and current_price == previous_price:
                    logging.info("Precio sin cambios. No se inserta historial.")
                    return last_entry["_id"]
                inserted_id = collection.insert_one(payload_).inserted_id
                logging.info("Nuevo historial insertado.")
                return inserted_id
            except Exception as e:
                logging.error(f"Error manejando 'products_history': {str(e)}")
                return None
        else:
            try:
                inserted_id = collection.insert_one(payload_).inserted_id
                return inserted_id
            except DuplicateKeyError:
                payload_ = {k: v for k, v in payload_.items() if k != "_id"}
                updated = collection.find_one_and_update(
                    filtro,
                    {"$set": payload_},
                    return_document=True
                )
                return updated["_id"]
            except Exception as e:
                logging.error(f"Error al insertar en '{collection_}': {e}")
                return None

    def load_data(self, dict__: dict):
        if dict__:
            new_product = {
                "title": dict__["title"],
                "url": dict__["url"],
                "condition": dict__["condition"],
                "brand_id": dict__["brand"],
                "main_category": dict__["main_category"],
                "category": dict__["category"],
                "sub_category": dict__["sub_category"],
                "warranty": dict__["warranty"],
                "payment_method": dict__["payment_method"],
                "seller": dict__["seller"],
                "delivery_time": dict__["delivery_time"],
                "delivery_cost": dict__["delivery_cost"],
                "specs": dict__['specs']
            }
            product = self.insert_data(dict__, 'products', 'url', 'url', new_product)

            new_price_doc = {
                "product_id": product,
                "extracted_at": datetime.now(),
                "original_price": dict__["original_price"],
                "price_with_discount": dict__["price_with_discount"],
                "discount_aplicated": dict__["discount_aplicated"],
                "stock": dict__["stock"],
                "total_solds": dict__["total_solds"],
                "recommendation": dict__["recommendation"],
                "rating": dict__["rating"],
                "total_califications": dict__["total_califications"],
                "quality_price_relation": dict__["quality_price_relation"]
            }
            price_doc = self.insert_data(
                {"product_id": product},
                "products_history",
                "product_id",
                "product_id",
                new_price_doc
            )
        else:
            logging.error(f"{dict__} is empty")
    
if __name__ == '__main__':
    scrapper = Scrapper(BASE_URL, client, 'ETL_Mercado_Libre')
    products_links = scrapper.get_links()
    for item in products_links:
        product_name = item['name']
        product_brand = item['brand']
        product_url = item['link']
        product_seller = item['seller']
        product_data = scrapper.fetch_data_from_link(product_url, product_name, product_brand, product_seller)
        scrapper.load_data(product_data)
import json
import logging
import os
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient

from config.db import DbConnection

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

client = MongoClient("mongodb://localhost:27017/")
db = client['ETL_Mercado_Libre']
conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv('SQL_SERVER_HOST')};DATABASE={os.getenv('SQL_SERVER_DB')};UID={os.getenv('SQL_SERVER_USER')};PWD={os.getenv('SQL_SERVER_PASS')}"

class Etl:
    def __init__(self, client_, db_):
        self.client: MongoClient = client_
        self.db = client[db_]
        self.all_products = pd.DataFrame()
        self.db_conn = DbConnection(conn_string)

    def extract_data(self, page, limit) -> list[dict]:
        skip = (page-1)*limit
        pipepline = [
            { "$sample": { "size": 10 } },
            {
                "$lookup": {
                    "from": "products_history_2",
                    "let": { "productId": "$_id" },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        { "$eq": ["$product_id", "$$productId"] }
                                    ]
                                }
                            }
                        },
                        { "$sort": { "extracted_at": -1 } },
                    ],
                    "as": "price_history"
                }
            },
            {
            "$match": {
                    "price_history": { "$ne": [] }
                }
            },
            { "$skip": skip },
            { "$limit": limit }
        ]
        results = list(self.db.products_2.aggregate(pipepline))
        results_formatted = [
            json.dumps(product, default=str, ensure_ascii=False, indent=2)
            for product in results
        ]
        rows = []
        for doc in results_formatted:
            doc = json.loads(doc)
            product_base = {k: v for k, v in doc.items() if k != "price_history"}
            for hist in doc["price_history"]:
                combined = {**product_base, **hist}
                rows.append(combined)
        return rows


    def insert_data(self, _cursor_, field_value: str, field_name: str, table: str, fields: tuple, values: tuple) -> int:
        # field_value = field_value.replace("'", "''")
        _cursor_.execute(f"SELECT id FROM {table} WHERE {field_name} = ?", field_value)
        row = _cursor_.fetchone()
        if row:
            logging.info(f"{field_value} ya existe en {table}, se retorna su ID existente.")
            return row[0]
        fields_str = f"({', '.join(fields)})"
        placeholders = ", ".join(["?"] * len(values))
        query = f"INSERT INTO {table} {fields_str} OUTPUT INSERTED.id VALUES ({placeholders})"
        _cursor_.execute(query, values)
        inserted_id = _cursor_.fetchone()[0]
        self.db_conn.conn.commit()
        logging.info(f"Insertado {field_value} en {table}, ID generado: {inserted_id}")
        return inserted_id
    
    def clean_decimal(self, value: str, max_digits: int = 18, decimals: int = 2) -> float | None:
        try:
            if not value or value.strip() == "":
                return None
            value_clean = value.replace(".", "").replace(",", ".")
            number = float(value_clean)
            max_whole = 10 ** (max_digits - decimals) - 1
            if abs(number) > max_whole:
                logging.warning(f"⚠️ Valor excede el rango DECIMAL({max_digits},{decimals}): {number}")
                return None
            return round(number, decimals)
        except ValueError:
            logging.warning(f"❌ No se pudo convertir: {value}")
            return None

    def load_data(self, row: dict):
        cursor = self.db_conn.get_cursor()

        #INSERTAMOS DATOS EN LA TABLA Brands
        brand_id = self.insert_data(
            cursor, 
            row['brand_id'], 
            "name", 
            "Brands", 
            ("name",), 
            (row["brand_id"],)
        )

        #INSERTAMOS DATOS EN LA TABLA MainCategories
        main_category_id = self.insert_data(
            cursor, row['main_category'], 
            "name", 
            "MainCategories", 
            ("name",), 
            (row["main_category"],)
        )

        #INSERTAMOS DATOS EN LA TABLA Categories
        category_id = self.insert_data(
            cursor, 
            row['category'], 
            "name", 
            "Categories", 
            ("name", "main_category_id",), 
            (row["category"], main_category_id,)
        )

        #INSERTAMOS DATOS EN LA TABLA SubCategories
        self.insert_data(
            cursor, 
            row['sub_category'], 
            "name", 
            "SubCategories", 
            ("name", "category_id",), 
            (row["sub_category"], category_id,)
        )

        #INSERTAMOS DATOS EN LA TABLA Products
        product_id = self.insert_data(
            cursor, 
            row['url'], 
            "url", 
            "Products", 
            ("title", "url", "condition", "brand_id", "main_category_id", "warranty", "payment_method", "seller", "delivery_time", "delivery_cost",), 
            (row["title"], row["url"], row["condition"], brand_id, main_category_id, row["warranty"], row["payment_method"], row["seller"], row["delivery_time"], row["delivery_cost"],)
        )

        #INSERTAMOS DATOS EN LA TABLA PriceHistory
        original_price = self.clean_decimal(row["original_price"])
        price_with_discount = self.clean_decimal(row["price_with_discount"])
        rating = self.clean_decimal(row["rating"], max_digits=3, decimals=2)
        self.insert_data(
            cursor, 
            product_id, 
            "product_id", 
            "PriceHistory", 
            ("product_id", "extracted_at", "original_price", "price_with_discount", "discount_aplicated", "stock", "total_solds", "recommendation", "rating", "total_califications", "quality_price_relation",), 
            (product_id, row["extracted_at"] or None, original_price, price_with_discount, row["discount_aplicated"] or None, row["stock"] or None, row["total_solds"] or None, row["recommendation"] or None, rating, row["total_califications"] or None,row["quality_price_relation"] or None,)
        )
            
if __name__ == '__main__':
    etl = Etl(client, 'ETL_Mercado_Libre')
    all_products = etl.extract_data(1, 50)
    for row in all_products:
        logging.info(f"---------------------------------------------------------------EACH ROW: \n{row}----------------------------------------------------------------")
        etl.load_data(row)
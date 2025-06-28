import json
import logging
import pandas as pd
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

client = MongoClient("mongodb://localhost:27017/")
db = client['ETL_Mercado_Libre']

class Etl:
    def __init__(self, client_, db_):
        self.client: MongoClient = client_
        self.db = client[db_]
        self.all_products = pd.DataFrame()

    def extract_data(self, page, limit):
        skip = (page-1)*limit
        pipepline = [
            {
                "$lookup": {
                    "from": "products_history",
                    "let": { "productId": "$_id" },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        { "$eq": ["$product_id", "$$productId"] },
                                        {
                                            "$gte": [
                                                "$extracted_at",
                                                { "$dateSubtract": { "startDate": "$$NOW", "unit": "day", "amount": 7 } }
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        { "$sort": { "extracted_at": -1 } },
                        { "$limit": 1 }
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
        results = list(self.db.products.aggregate(pipepline))
        results_formatted = [
            json.dumps(product, default=str, ensure_ascii=False, indent=2)
            for product in results
        ]
        rows = []
        for doc in results_formatted:
            logging.info("--------------------------------------------------EACH PRODUCT--------------------------------------------------")
            logging.info("----------------------------------------------------------------------------------------------------------------")
            doc = json.loads(doc)
            logging.info(doc)
            product_base = {k: v for k, v in doc.items() if k != "price_history"}
            for hist in doc["price_history"]:
                combined = {**product_base, **hist}
                rows.append(combined)
        df_products = pd.DataFrame(rows)
        print(df_products.columns)
        print(df_products.head())

if __name__ == '__main__':
    etl = Etl(client, 'ETL_Mercado_Libre')
    etl.extract_data(1, 5)
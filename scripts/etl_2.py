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
                    "from": "products_history_2",
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
                        # { "$limit": 1 }
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
        return df_products
    
    def transform_data(self, df_: pd.DataFrame):
        for col in df_:
            if 'url' in col:
                df_.drop(col, axis=1, inplace=True)
            if '_id' in col:
                df_.drop(col, axis=1, inplace=True)
            if 'specs' in col:
                df_.drop(col, axis=1, inplace=True)
        print(f"Columns after dropping url, specs and _id: {df_.columns}")
        print(f"Columns type: \n{df_.dtypes}")
        df_["original_price"] = pd.to_numeric(df_["original_price"].str.replace(",", "", regex=False).str.replace(".", "", regex=False), errors="coerce")
        df_["price_with_discount"] = pd.to_numeric(df_["price_with_discount"].str.replace(",", "", regex=False).str.replace(".", "", regex=False), errors="coerce")
        df_["discount_aplicated"] = pd.to_numeric(df_["discount_aplicated"].str.replace("% OFF", "", regex=False), errors="coerce")
        df_["extracted_at"] = pd.to_datetime(df_["extracted_at"])
        df_["rating"] = pd.to_numeric(df_["rating"], errors="coerce")
        df_["total_califications"] = pd.to_numeric(df_["total_califications"].str.replace(" calificaciones", "", regex=False), errors="coerce")
        print(f"Final columns type: \n{df_.dtypes}")
        return df_

if __name__ == '__main__':
    etl = Etl(client, 'ETL_Mercado_Libre')
    all_products = etl.extract_data(1, 50)
    all_products_transformed = etl.transform_data(all_products)
    all_products_transformed.to_csv("./all_products_transformed.csv", encoding="utf-8-sig", sep=";", index=False)
# product_utils.py
import pandas as pd
import logging
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def fetch_product_titles(**kwargs):
    ti = kwargs['ti']
    conn = BaseHook.get_connection("postgres_def")
    postgres_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(postgres_url)
    
    try:
        # Check if the products table exists
        query = f"SELECT distinct title, price FROM products"
        df = pd.read_sql(query, engine)
        
        if df.empty:
            logger.warning("Products table is empty. Using default product list.")
            products = [
                ("Apple", 0.5), ("Banana", 0.3), ("Milk", 2.0), ("Bread", 1.5),
                ("Eggs", 3.0), ("Chicken", 5.0), ("Rice", 4.0), ("Pasta", 2.5)
            ]
        else:
            # Convert DataFrame to list of tuples (title, price)
            products = list(zip(df['title'].tolist(), df['price'].tolist()))
            logger.info(f"Fetched {len(products)} products from database")
        
        # Push products to XCom
        ti.xcom_push(key="product_list", value=products)
        return products
        
    except Exception as e:
        logger.error(f"Error fetching products: {e}", exc_info=True)
        # Return default products as fallback
        default_products = [
            ("Apple", 0.5), ("Banana", 0.3), ("Milk", 2.0), ("Bread", 1.5),
            ("Eggs", 3.0), ("Chicken", 5.0), ("Rice", 4.0), ("Pasta", 2.5)
        ]
        ti.xcom_push(key="product_list", value=default_products)
        return default_products
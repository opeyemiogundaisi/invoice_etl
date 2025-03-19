from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
import pandas as pd
from faker import Faker
from airflow.utils.email import send_email
import logging
import random
import os
import requests
from sqlalchemy import create_engine
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timedelta, datetime

# Define Connections and variables to be used
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1RoB52Rk-71uQiuplol7yhLvgWHFv3v079u-yMEQEu8o/edit?usp=sharing"
POSTGRES_CONN = "postgres_def"
TABLE_NAME = "transaction table"
FAKE_TABLE_NAME= "fake_invoices"
PRODUCTS_TABLE_NAME= "products"
REVIEWS_TABLE_NAME= "reviews"
logger = logging.getLogger(__name__)

def generate_fake_data(**kwargs):
    ti = kwargs['ti']
    fake = Faker()
    try:
        output_folder = "/opt/airflow/generated_data"
        os.makedirs(output_folder, exist_ok=True)

        products = [
            ("Apple", 0.5), ("Banana", 0.3), ("Milk", 2.0), ("Bread", 1.5),
            ("Eggs", 3.0), ("Chicken", 5.0), ("Rice", 4.0), ("Pasta", 2.5)
        ]

        transaction_data = []
        for _ in range(700):
            transaction_data.append([
                f"INV{fake.unique.random_int(min=100000, max=999999)}",
                fake.date_between(start_date="-1y", end_date="today"),
                fake.name(),
                random.choice(products)[0],
                random.randint(1, 10),
                random.choice(products)[1],
                round(random.randint(1, 10) * random.choice(products)[1], 2),
                random.choice(["PostgreSQL", "Google Sheets", "CSV"]),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ])

        df = pd.DataFrame(transaction_data, columns=[
            "Invoice Number", "Invoice Date", "Customer Name",
            "Product Name", "Quantity", "Unit Price", "Total Amount", "Platform", "Last Update"
        ])

        output_path = os.path.join(output_folder, f"supermarket_transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        df.to_csv(output_path, index=False)
        
        # Push file path to XCom
        ti.xcom_push(key="csv_path", value=output_path)

        logger.info(f"File saved to: {output_path}")

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)


#Push to sheet
def push_to_google_sheets(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='generate_data', key="csv_path")

    if not file_path:
        logger.error("No file path found from generate_data task")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/opt/airflow/jireh-ope-9348c4d91fd9.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(GOOGLE_SHEET_URL).sheet1

    df = pd.read_csv(file_path)
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())

    logger.info(f"Data pushed to Google Sheet from {file_path}")


# Extract Sheet
def extract_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/opt/airflow/jireh-ope-9348c4d91fd9.json", scope)
    client = gspread.authorize(creds)
    
    sheet = client.open_by_url(GOOGLE_SHEET_URL).sheet1
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    df.to_csv("/opt/airflow/google_sheets_data.csv", index=False)

#Load to postgres
def load_to_postgres():
    df = pd.read_csv("/opt/airflow/google_sheets_data.csv")
    conn = BaseHook.get_connection("postgres_def")
    postgres_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(postgres_url)
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)


#Send Load success mail
def notify_success(context):
    subject = "Airflow Alert: Data Pulled to Google Sheets"
    body = "The data pull to Google Sheets was successful!"
    send_email(to="opsyyjoe@gmail.com", subject=subject, html_content=body)
    

#Fetch Fake Invoices
def fetch_fake_cart(**kwargs):
    url = "https://dummyjson.com/carts"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()["carts"] 
        invoices = []  

        # Flatten the nested JSON
        for cart in data:
            cart_id = cart["id"]
            user_id = cart["userId"]
            total = cart["total"]
            discounted_total = cart["discountedTotal"]
            total_products = cart["totalProducts"]
            total_quantity = cart["totalQuantity"]

            # Extract products within each cart
            for product in cart["products"]:
                invoices.append({
                    "cart_id": cart_id,
                    "user_id": user_id,
                    "total": total,
                    "discounted_total": discounted_total,
                    "total_products": total_products,
                    "total_quantity": total_quantity,
                    "product_id": product["id"],
                    "product_title": product["title"],
                    "product_price": product["price"],
                    "product_quantity": product["quantity"],
                    "product_total": product["total"],
                    "discount_percentage": product["discountPercentage"],
                    "discounted_price": product["discountedTotal"],
                    "thumbnail": product["thumbnail"],
                    "load_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

        # Convert to DataFrame
        df = pd.DataFrame(invoices)

        # Save the structured data
        file_path = "/opt/airflow/fake_invoices.csv"
        df.to_csv(file_path, index=False)
        kwargs['ti'].xcom_push(key="invoice_data_path", value=file_path)

    else:
        raise Exception(f"Failed to fetch invoice data: {response.status_code}")

def load_fake_invoice_to_postgres(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='fetch_fake_invoice', key="invoice_data_path")
    logger.info(f"Loading invoice data from: {file_path}")
    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found at: {file_path}")
    df = pd.read_csv(file_path)
    conn = BaseHook.get_connection("postgres_def")
    postgres_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(postgres_url)
    df.to_sql(FAKE_TABLE_NAME, engine, if_exists='replace', index=False)
    logger.info("Fake invoice data loaded successfully into PostgreSQL")

#Fetch products and reviews
def fetch_fake_product(**kwargs):
    url = "https://dummyjson.com/products"
    response = requests.get(url)
    data = response.json()["products"]
    products_list = [] 
    reviews_list = []

    for product in data:
        products_info = {
            "id": product["id"],
            "title": product["title"],
            "description": product["description"],
            "category": product["category"],
            "price": product["price"],
            "discountPercentage": product["discountPercentage"],
            "rating": product.get("rating"),
            "stock": product.get("stock"),
            "brand": product.get("brand"),
            "sku": product.get("sku")
        }
        products_list.append(products_info)

    if "reviews" in product and product["reviews"]:
        for review in product["reviews"]:
            review_info = {
                "product_id": product["id"],
                "rating": review["rating"],
                "comment": review["comment"],
                "date": review["date"],
                "reviewer_name": review["reviewerName"],
                "reviewer_email": review["reviewerEmail"]
            }
            reviews_list.append(review_info)

        products_list.append(products_info)

        products_df=pd.DataFrame(products_list)
        reviews_df=pd.DataFrame(reviews_list)


        # Save the structured data
        products_file_path = "/opt/airflow/products.csv"
        reviews_file_path = "/opt/airflow/reviews.csv"
        products_df.to_csv(products_file_path, index=False)
        reviews_df.to_csv(reviews_file_path, index=False)
        kwargs['ti'].xcom_push(key="product_data_path", value=products_file_path)
        kwargs['ti'].xcom_push(key="reviews_data_path", value=reviews_file_path)


def load_products_to_postgres(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='fetch_fake_product', key="product_data_path")
    logger.info(f"Loading products data from: {file_path}")
    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found at: {file_path}")
    df = pd.read_csv(file_path)
    conn = BaseHook.get_connection("postgres_def")
    postgres_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(postgres_url)
    df.to_sql(PRODUCTS_TABLE_NAME, engine, if_exists='replace', index=False)
    logger.info("Products loaded successfully into PostgreSQL")

def load_reviews_to_postgres(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='fetch_fake_product', key="reviews_data_path")
    logger.info(f"Loading reviews data from: {file_path}")
    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found at: {file_path}")
    df = pd.read_csv(file_path)
    conn = BaseHook.get_connection("postgres_def")
    postgres_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(postgres_url)
    df.to_sql(REVIEWS_TABLE_NAME, engine, if_exists='replace', index=False)
    logger.info("Reviews loaded successfully into PostgreSQL")


# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'execution_timeout': timedelta(minutes=2)
}

dag = DAG(
    'google_sheets_to_postgres_vp',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)


generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=generate_fake_data,
    provide_context=True,
    dag=dag,
)


push_to_sheet = PythonOperator(
    task_id='push_to_sheet',
    python_callable=push_to_google_sheets,
    execution_timeout=timedelta(minutes=1),
    provide_context=True,
    on_success_callback=notify_success,
    dag=dag
)

extract_sheet= PythonOperator(
    task_id='extract_google_sheets',
    python_callable=extract_google_sheets,
    dag=dag,
)


load_postgres = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)


fetch_cart = PythonOperator(
    task_id='fetch_fake_invoice',
    python_callable=fetch_fake_cart,
    dag=dag,
)

load_cart_db = PythonOperator(
    task_id='load_fake_invoice',
    python_callable=load_fake_invoice_to_postgres,
    dag=dag,
)

fetch_fake_products = PythonOperator(
    task_id = 'fetch_fake_product',
    python_callable=fetch_fake_product,
    dag=dag,
)

load_products_db = PythonOperator(
    task_id = 'load_products_to_postgres',
    python_callable=load_products_to_postgres,
    dag=dag,
)

load_reviews_db = PythonOperator(
    task_id = 'load_reviews_to_postgres',
    python_callable=load_reviews_to_postgres,
    dag=dag,
)


#Load should happen at the same time for the sake of accuracy in db
generate_data >> push_to_sheet >> extract_sheet
extract_sheet >> fetch_cart
fetch_cart >> fetch_fake_products
fetch_fake_products >> [load_products_db, load_reviews_db, load_postgres, load_cart_db]
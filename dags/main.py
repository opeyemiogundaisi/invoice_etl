from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
import pandas as pd
from faker import Faker
import logging
import random
import os
from sqlalchemy import create_engine
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timedelta, datetime

# Define Connections and variables to be used
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1RoB52Rk-71uQiuplol7yhLvgWHFv3v079u-yMEQEu8o/edit?usp=sharing"
POSTGRES_CONN = "postgres_def"
TABLE_NAME = "transaction table"
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



# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'execution_timeout': timedelta(minutes=2)
}

dag = DAG(
    'google_sheets_to_postgres_vpro',
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




generate_data >> push_to_sheet >> extract_sheet >> load_postgres

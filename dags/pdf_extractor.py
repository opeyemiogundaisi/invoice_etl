import pdfplumber
import pandas as pd
import os

def extract_customer_data_from_pdf(**kwargs):
    ti = kwargs['ti']
    pdf_path = "/opt/airflow/data/customer_data_pdf.pdf"

    data = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                for row in table:
                    data.append(row)
    
    df = pd.DataFrame(data, columns=["Customer ID", "Customer Name", "Email", "Phone", "Address"])
    output_path = "/opt/airflow/data/customer_data.csv"
    df.to_csv(output_path, index=False)

    ti.xcom_push(key="customer_csv_path", value=output_path)
    print(f"Extracted customer data saved at: {output_path}")

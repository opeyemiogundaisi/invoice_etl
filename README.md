# 🛒 Retail Data ETL Pipeline with Airflow, APIs, PDF & Google Sheets

An enterprise-style ETL pipeline built with **Apache Airflow** to ingest, transform, and load product, invoice, and customer data from various sources including **DummyJSON APIs**, **PDF documents**, **Google Sheets**, and **PostgreSQL**. This project simulates real-world e-commerce workflows including invoice generation, customer enrichment, and product review ingestion.

---

## 🔁 Workflow Summary

The Airflow DAG `business_logic1_v2` orchestrates the following tasks:

### 💼 Data Sources
- **DummyJSON API** – Product, Cart, and Review data
- **PDF Files** – Customer contact and profile details
- **Google Sheets** – Shared invoice ledger (read/write)
- **Faker Library** – Synthetic invoice generation

### 🔧 Core Pipeline Tasks
1. **Extract Customer Data from PDF**
   - Parses structured tables from PDFs using `pdfplumber`.

2. **Fetch Product Names**
   - Retrieves existing product list from PostgreSQL, or uses default.

3. **Generate Fake Invoice Data**
   - Uses Faker + pulled product data to simulate 1000 invoice records.

4. **Push Invoices to Google Sheets**
   - Writes invoice data into a live shared sheet.

5. **Extract Google Sheet into CSV**
   - Pulls the latest Google Sheets content for DB loading.

6. **Load Google Sheet to PostgreSQL**
   - Appends verified data into `invoice` table.

7. **Fetch Cart Data from API**
   - Pulls and flattens nested cart + product details from DummyJSON.

8. **Fetch Product & Review Data**
   - Extracts detailed product metadata and customer reviews from API.

9. **Load Everything to PostgreSQL**
   - Loads cleaned datasets into `products`, `reviews`, `cart`, `customers`, `invoice`.

10. **Email Notification**
    - Sends a success email on sheet sync.

---

## 📦 Final PostgreSQL Tables

| Table            | Description                                      |
|------------------|--------------------------------------------------|
| `invoice`        | Cleaned invoice records from Google Sheets       |
| `cart`           | Flattened shopping cart and product details      |
| `products`       | Full product catalog with brand/category info    |
| `reviews`        | Customer review and rating data                  |
| `customers`      | Extracted customer details from PDF              |

---

## 📁 Folder & Code Structure


---

## 🛠 Tech Stack

- **Apache Airflow** – Pipeline orchestration  
- **Python** – Data manipulation, API calls, PDF parsing  
- **PostgreSQL** – Central warehouse  
- **Google Sheets API** – Collaborative data storage  
- **Faker** – Fake data generation  
- **pdfplumber** – PDF data extraction  
- **DummyJSON** – Realistic e-commerce API data source  

---

## 🚀 Running the Pipeline

### Prerequisites
- Apache Airflow setup
- PostgreSQL connection `postgres_def` in Airflow
- Google Sheets service account credentials stored in Airflow `/opt/airflow/data/`
- Required Python packages: `pandas`, `gspread`, `pdfplumber`, `sqlalchemy`, `faker`, etc.

### Trigger the DAG
```bash
airflow dags trigger business_logic1_v2


# 🚕 NYC Taxi Analytics & GenAI Insights Platform

A comprehensive data engineering and analytics solution for NYC Yellow Taxi data (January 2015). This project demonstrates a full-stack data pipeline including ETL processing (Pandas & PySpark), SQL analytics, KPI computation, and an interactive Streamlit dashboard powered by Google Gemini AI.

## 📋 Project Overview

This platform is designed to ingest, clean, and analyze high-volume urban mobility data. It processes over **12 million trip records** to uncover insights regarding peak demand, revenue optimization, and traffic patterns.

### Key Features

* **Dual ETL Pipelines:** Includes both a Pandas-based pipeline for local processing and a **PySpark** pipeline demonstrating scalability for Big Data (100GB+).
* **Interactive Dashboard:** A Streamlit web app with Plotly visualizations for real-time data exploration.
* **GenAI Assistant:** Integrated **Google Gemini** chatbot that answers natural language questions about the dataset (e.g., *"What time should I drive to maximize tips?"*).
* **SQL Query Lab:** A sandbox for executing custom SQL queries against the processed data.
* **Automated Reporting:** Scripts to generate static KPI reports and high-resolution plots.

---

## 📂 File Structure

| File Name | Description |
| --- | --- |
| **`Mobility_data_analyser.py`** | **Step 1 (Local):** Class-based ETL pipeline using Pandas. Cleans raw CSV data, performs feature engineering, and outputs `cleaned_trips.parquet`. |
| **`pyspark_etl.py`** | **Step 1 (Scalable):** Distributed ETL pipeline using PySpark. Designed for performance and scalability, demonstrating how to handle massive datasets. |
| **`compute_kpis.py`** | **Step 2:** Loads processed data to calculate business KPIs (Revenue, Tips, Peak Hours) and generates static `.png` visualizations. |
| **`sql_analytics.py`** | **Step 3:** Connects to the SQLite database to run complex analytical queries and export results to CSVs. |
| **`streamlit_app.py`** | **Step 4 (Frontend):** The main dashboard application. Integrates data, visualizations, and the GenAI assistant. |
| `yellow_tripdata_2015-01.csv` | *Input:* The raw dataset (source file required). |
| `taxi_analytics.db` | *Output:* SQLite database used by the SQL Lab. |
| `cleaned_trips.parquet` | *Output:* The optimized data file used by the dashboard. |

---

## ⚙️ Prerequisites & Installation

### 1. System Requirements

* Python 3.8+
* Java 8 or 11 (Required for PySpark)
* Google Gemini API Key (for GenAI features)

### 2. Install Python Dependencies

Create a virtual environment and install the required libraries:

```bash
pip install pandas numpy streamlit plotly seaborn matplotlib pyspark google-generativeai tabulate openpyxl

```

### 3. Data Setup

Ensure the raw dataset `yellow_tripdata_2015-01.csv` is placed in the root directory of the project.

---

## 🚀 Execution Guide (Step-by-Step)

Follow these steps in order to build the pipeline and launch the application.

### Step 1: Data Processing (ETL)

You can choose between the Pandas implementation (faster for this specific file size) or the PySpark implementation (demonstration of scalability).

**Option A: Pandas (Recommended for Local Dev)**
Runs the cleaning and feature engineering pipeline.

```bash
python Mobility_data_analyser.py

```

* **Output:** Generates `cleaned_trips.parquet`.

**Option B: PySpark (Scalability Demo)**
Simulates a distributed computing environment.

```bash
python pyspark_etl.py

```

* **Output:** Generates `output/` folder with CSV/Parquet files and prints a scalability analysis.

### Step 2: Generate KPIs & Visualizations

Compute financial metrics and generate static charts for reports.

```bash
python compute_kpis.py

```

* **Output:** Prints KPIs to console and saves images like `comprehensive_dashboard.png`.

### Step 3: Run SQL Analytics

Execute predefined business queries against the database.
*Note: Ensure your `taxi_analytics.db` is populated. You may need to uncomment the `df.to_sql` lines in the script if running for the first time.*

```bash
python sql_analytics.py

```

* **Output:** Generates `analytics_queries.sql` and multiple CSV reports (e.g., `query1_peak_hours.csv`).

### Step 4: Launch the Dashboard

Start the interactive web application.

```bash
streamlit run streamlit_app.py

```

* **Action:** Opens the dashboard in your default web browser (usually http://localhost:8501).

---

## 💡 Dashboard User Guide

Once the Streamlit app is running, navigate using the sidebar:

1. **📊 Dashboard:** High-level executive summary with live metrics (Revenue, Trips, Tips) and interactive Plotly charts.
2. **🤖 GenAI Assistant:**
* Type questions like *"What is the revenue trend on weekends?"*
* The AI uses context from your data to provide specific, numeric answers.


3. **💾 SQL Query Lab:**
* Write and execute custom SQL queries directly on the dataset.
* Example: `SELECT hour, count(*) FROM trips GROUP BY hour`


4. **📈 Deep Dive:** Advanced tabs for Payment Analysis, Trip Patterns, and Financial Distribution.

---

## 🏗️ Scalability Architecture

For datasets exceeding **100GB**, the project includes `pyspark_etl.py` which transitions from single-node Pandas processing to distributed Spark clusters.

* **Storage:** Switches from local CSV to partitioned Parquet on S3/Data Lake.
* **Compute:** Utilizes Spark RDDs/DataFrames for parallel processing across nodes.
* **Optimization:** Implements partition pruning and caching strategies documented in the script.

---

## 🔑 API Configuration

The `streamlit_app.py` contains a placeholder for the Google Gemini API key. For production use, it is recommended to use environment variables:

1. Create a `.env` file or export the variable:
```bash
export GOOGLE_API_KEY="your_api_key_here"

```


2. Update line 248 in `streamlit_app.py`:
```python
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

```



---

*Built with ❤️ using Python, Streamlit, and Google Gemini.*

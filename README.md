# ETL Country GDP – Largest Banks 💰

An end-to-end **ETL (Extract, Transform, Load)** pipeline that extracts the *List of Largest Banks by Market Capitalization* from a Wikipedia page, converts the values into multiple currencies using exchange rates, and stores the processed data in both a **CSV file** and a **SQLite database**.

---

## 📊 Project Overview

This project demonstrates how to automate the extraction, transformation, and loading of data from a web source into structured storage using Python.
It includes robust logging, transformation logic, and database operations.

---

## 🧠 ETL Workflow

1. **Extract:**

   * Scrapes bank names and market capitalization (USD) from a Wikipedia page archived by Wayback Machine.

2. **Transform:**

   * Converts market capitalization into GBP, EUR, and INR using rates from a local CSV file.

3. **Load:**

   * Saves the transformed data into both a CSV file and an SQLite database.
   * Runs example SQL queries to validate the loaded data.

---

## ⚙️ Tech Stack

* **Python 3.10+**
* **Libraries:** pandas, requests, BeautifulSoup4, numpy, sqlite3
* **Data Source:** [Wikipedia - List of Largest Banks (Archived)](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)

---

## 🚀 How to Run the Project

### 1️⃣ Clone the repository

```bash
git clone https://github.com/yourusername/etl-country-gdp-banks.git
cd etl-country-gdp-banks
```

### 2️⃣ Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate     # On Mac/Linux
venv\Scripts\activate        # On Windows
```

### 3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Run the ETL script

```bash
python src/etl_banks.py
```

---

## 🧾 Example Output

**Console Output:**

```
Table is ready
SELECT * from Largest_banks
       Name       MC_USD_Billion  MC_GBP_Billion  MC_EUR_Billion  MC_INR_Billion
0   JPMorgan Chase      406.72          321.45          378.12        33915.26
1   ICBC              265.22          209.52          246.61        22099.01
...
```

**CSV and Database:**

* Output CSV → `./data/Largest_banks_data.csv`
* SQLite DB → `Banks.db`

---

## 📁 Project Structure

```
etl-country-gdp-banks/
├── src/etl_banks.py
├── data/exchange_rate.csv
├── logs/code_log.txt
├── requirements.txt
└── README.md
```

---

## 🔍 Example Queries Run

```sql
SELECT * FROM Largest_banks;
SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
SELECT Name FROM Largest_banks LIMIT 5;
```

---

## 🧑‍💻 Author

**Your Name**
🔗 [LinkedIn](https://linkedin.com/in/your-linkedin) | 🌐 [Portfolio](your-portfolio-link)

---

## 💡 Future Enhancements

* Integrate with **Airflow** or **Prefect** for scheduling
* Add **Docker support**
* Store output in a **cloud database (PostgreSQL / AWS RDS)**
* Add **unit tests** for validation

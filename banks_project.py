# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import requests
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime 


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_name = 'Largest_banks'
db_name = 'Banks.db'
csv_path = './data/Largest_banks_data.csv'
exchange_rate = './data/exchange_rate.csv'
log_file = "./logs/code_log.txt" 
table_attributes = ['Name', 'MC_USD_Billion']
tables_atrribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    # Year-Monthname-Day-Hour-Minute-Second 
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    # current datetime
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ':' + message + '\n') 


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text

    # Parse the text using BeautifulSoup to extract relevant information
    data = BeautifulSoup(html_page, 'html.parser')

    # create an empty data frame to hold extracted data
    extracted_data = pd.DataFrame(columns=table_attributes)

    # Access rows of the table
    tables = data.find_all("tbody")
    rows = tables[0].find_all("tr")

    # iterate over the rows to find the required data
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            name_tag = col[0].find('a')
            name = name_tag.text.strip() if name_tag else col[1].text.strip()

            mc_usd = col[2].get_text(strip=True).replace('\n', '').replace(',', '')

            data_dict = {"Name": name,
                             "MC_USD_Billion": float(mc_usd)}
            df1 = pd.DataFrame(data_dict, index=[0])
            extracted_data = pd.concat([extracted_data,df1], ignore_index=True)

    return extracted_data


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    df_er = pd.read_csv(exchange_rate)
    dict_df = df_er.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x*dict_df['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*dict_df['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*dict_df['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(csv_path)    


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)
    print('Table is ready')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Log the initialization of the ETL process 
log_progress("Preliminaries complete. Initiating ETL process.") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract(url, table_attributes) 
 
# Log the completion of the Extraction process 
log_progress("Data extraction complete. Initiating Transformation process.") 
 
# Log the beginning of the Transformation process 
transformed_data = transform(extracted_data, exchange_rate) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Data transformation complete. Initiating loading process.") 
 
# Log the beginning of the Loading process 
log_progress("Loading data to CSV") 
load_to_csv(transformed_data, csv_path)
log_progress("Data saved to CSV file.") 

log_progress("Initiating Connection to SQL")
sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated.")

log_progress("Loading data to Database") 
load_to_db(transformed_data, sql_connection, table_name)
log_progress("Data loaded to Database as table. Running the query.")

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)


query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress("Process Complete.")
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 

sql_connection.close()

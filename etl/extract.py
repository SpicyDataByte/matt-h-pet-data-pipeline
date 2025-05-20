import pandas as pd
def extract_xlsx():
    file_path = '/opt/airflow/data/data.xlsx'
    raw_data = pd.read_excel(file_path, sheet_name=0)
    raw_data.to_csv('/opt/airflow/data/raw_client_pet_data.csv', index=False)
    

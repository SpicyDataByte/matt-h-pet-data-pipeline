import pandas as pd
import re
from etl.transform_helpers import *

# --------------------
# File Path Constants
# --------------------

RAW_DATA_PATH = '/opt/airflow/data/raw_client_pet_data.csv'
CLEAN_COLUMNS_PATH = '/opt/airflow/data/cleaned/clean_columns_client_data.csv'

# --------------------
# Transform Functions
# --------------------

def clean_columns():
    df = pd.read_csv(RAW_DATA_PATH)
    df.columns = df.columns.str.replace(' ', '_').str.rstrip('_').str.lower()
    df.rename(columns={
        'last_mane': 'last_name',
        'owner_first': 'first_name',
        'expiry_year': 'license_expiry',
        'number': 'street_number',
        'city': 'raw_city',
        'province': 'raw_province',
        'owner_phone': 'raw_owner_phone',
        'tag_#': 'tag_number'
    }, inplace=True)
    df.to_csv(CLEAN_COLUMNS_PATH, index=False)
    
def convert_column_types():
    df = pd.read_csv(CLEAN_COLUMNS_PATH)
    for col in df.columns:
        if df[col].dtype.name not in ['datetime64[ns]', 'object']:
            df[col] = df[col].astype('Int64')
    for col in df.columns:
        if df[col].dtype.name not in ['datetime64[ns]', 'string', 'Int64']:
            df[col] = df[col].astype('string')
    df.to_csv(CLEAN_COLUMNS_PATH, index=False)
    
def categorize_city_column():
    df = pd.read_csv(CLEAN_COLUMNS_PATH)
    df['city'] = df['raw_city'].apply(categorize_city)
    df.to_csv(CLEAN_COLUMNS_PATH, index=False)


def categorize_province_column():
    df = pd.read_csv(CLEAN_COLUMNS_PATH)
    df['province'] = df['raw_province'].apply(categorize_province)
    df.to_csv(CLEAN_COLUMNS_PATH, index=False)


def standardize_owner_phone_column():
    df = pd.read_csv(CLEAN_COLUMNS_PATH)
    df['owner_phone'] = df['raw_owner_phone'].apply(standardize_phone)
    df.to_csv(CLEAN_COLUMNS_PATH, index=False)


def infer_missing_provinces_from_phone():
    df = pd.read_csv(CLEAN_COLUMNS_PATH)
    df['inferred_province'] = df.apply(infer_missing_province, axis=1)
    df.to_csv(CLEAN_COLUMNS_PATH, index=False)


def standardize_pet_species_column():
    df = pd.read_csv(CLEAN_COLUMNS_PATH)
    df['pet_species'] = df['pet_species'].apply(standardize_species)
    df.to_csv(CLEAN_COLUMNS_PATH, index=False)
    
def standardize_unit_column():
    df = pd.read_csv(CLEAN_COLUMNS_PATH)
    df['unit'] = df['unit'].apply(standardize_unit)
    df.to_csv(CLEAN_COLUMNS_PATH, index=False)

def handle_missing_values():
    df = pd.read_csv(CLEAN_COLUMNS_PATH)
    df[df.select_dtypes(include='number').columns] = df.select_dtypes(include='number').fillna(0)
    df[df.select_dtypes(include='string').columns] = df.select_dtypes(include='string').fillna('Unknown')
    df[df.select_dtypes(include='datetime').columns] = df.select_dtypes(include='datetime').fillna(pd.Timestamp('1900-01-01'))
    df.to_csv(CLEAN_COLUMNS_PATH, index=False)












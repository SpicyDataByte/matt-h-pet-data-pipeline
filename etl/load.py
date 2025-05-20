import pandas as pd
import sqlalchemy
import hashlib
from sqlalchemy import create_engine, text

def generate_id_from_hash(row, columns):
    """
    Generate a consistent integer ID from a hash of the specified columns.
    
    Example:
    >>> generate_id_from_hash(row, ['first_name', 'last_name', 'email'])
        305293538
    """
    concat_str = '|'.join(str(row[col]) for col in columns)
    hash_val = hashlib.sha256(concat_str.encode('utf-8')).hexdigest()
    return int(hash_val[:8], 16) 

def add_surrogate_keys():
    df = pd.read_csv('/opt/airflow/data/validated/final_valid_records.csv')

    
    owner_id_cols = ['first_name', 'last_name', 'owner_email', 'last_renewal_date']
    pet_id_cols = owner_id_cols + ['tag_number', 'pet_species', 'pet_colour']

    df['pet_owner_id'] = df.apply(lambda row: generate_id_from_hash(row, owner_id_cols), axis=1)
    df['pet_id'] = df.apply(lambda row: generate_id_from_hash(row, pet_id_cols), axis=1)
    
    species_df = df[['pet_species']].drop_duplicates().rename(columns={'pet_species': 'species_name'})
    breed_df = df[['pet_breed']].drop_duplicates().rename(columns={'pet_breed': 'breed_name'})
    colour_df = df[['pet_colour']].drop_duplicates().rename(columns={'pet_colour': 'colour_name'})
    
    species_df['species_id'] = species_df.index + 1
    breed_df['breed_id'] = breed_df.index + 1
    colour_df['colour_id'] = colour_df.index + 1
    
    df = df.merge(species_df, left_on='pet_species', right_on='species_name', how='left')
    df = df.merge(breed_df, left_on='pet_breed', right_on='breed_name', how='left')
    df = df.merge(colour_df, left_on='pet_colour', right_on='colour_name', how='left')
    
    df.to_csv('/opt/airflow/data/validated/final_with_ids.csv', index=False)

def truncate_tables():
    tables = [
        'license',
        'pets',
        'pet_owners',
        'pet_breed',
        'pet_colour',
        'pet_species'
    ]
    engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/pet_database')
    
    with engine.begin() as conn:
        for table in tables:
            print(f"Truncating table: {table}")
            conn.execute(text(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;'))


def load_normalized_tables():
    df = pd.read_csv('/opt/airflow/data/validated/final_with_ids.csv')
    engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/pet_database')
    
    pet_owner_df = df[[
        'pet_owner_id',
        'first_name', 'last_name',                
        'street_number', 'street_name', 'unit',
        'city', 'inferred_province', 'postal_code',
        'owner_phone', 'owner_email', 'alternate_phone'
    ]].drop_duplicates().rename(columns={
        'first_name': 'owner_first_name',
        'last_name': 'owner_last_name',
        'street_number': 'owner_street_number',
        'street_name': 'owner_street_name',
        'unit': 'owner_unit',
        'city': 'owner_city',
        'inferred_province': 'owner_province',
        'postal_code': 'owner_postal_code',
        'alternate_phone': 'owner_alternate_phone'
    })


    pets_df = df[[
        'pet_id', 'pet_owner_id', 'pet_name',
        'species_id', 'breed_id', 'colour_id',
        'pet_sex', 'pet_is_fixed', 'pet_status'
    ]].drop_duplicates().rename(columns={
        'pet_sex': 'sex',
        'pet_is_fixed': 'is_fixed'
    })
    
    license_df = df[[
        'pet_id', 'tag_number', 'last_renewal_date', 'license_expiry', 'tag_status'
    ]].drop_duplicates().rename(columns={
        'tag_number': 'license_tag_number',
        'last_renewal_date': 'last_date_issued'
    })
    license_df['last_date_issued'] = pd.to_datetime(license_df['last_date_issued'], errors='coerce')
    license_df['license_expiry'] = pd.to_datetime(license_df['license_expiry'], errors='coerce')
    license_df['license_tag_number'] = pd.to_numeric(license_df['license_tag_number'], errors='coerce').astype('Int64')
    
    species_df = df[['species_id', 'species_name']].drop_duplicates(subset=['species_id'])
    breed_df = df[['breed_id', 'breed_name']].drop_duplicates(subset=['breed_id'])
    colour_df = df[['colour_id', 'colour_name']].drop_duplicates(subset=['colour_id'])

    
    species_df.to_sql('pet_species', engine, if_exists='append', index=False)
    breed_df.to_sql('pet_breed', engine, if_exists='append', index=False)
    colour_df.to_sql('pet_colour', engine, if_exists='append', index=False)

    pet_owner_df.to_sql('pet_owners', engine, if_exists='append', index=False)
    pets_df.to_sql('pets', engine, if_exists='append', index=False)
    license_df.to_sql('license', engine, if_exists='append', index=False)
    
    df_all = pd.read_csv('/opt/airflow/data/validated/all_records.csv')
    df_all.to_sql('pets_wide_table', engine, if_exists='replace', index=False)
    
    df_invalid = pd.read_csv('/opt/airflow/data/validated/invalid_records.csv')
    df_invalid.to_sql('pets_invalid_records', engine, if_exists='replace', index=False)
import pandas as pd

class ColumnValidator:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.df['failed_columns'] = [[] for _ in range(len(df))] 

    def validate(self, column: str, test_func):
        valid_mask = self.df[column].apply(test_func)
        self.df.loc[~valid_mask, 'failed_columns'] = self.df.loc[~valid_mask, 'failed_columns'].apply(
            lambda current: current + [column]
        )

    def get_valid_rows(self):
        return self.df[self.df['failed_columns'].apply(len) == 0].copy()

    def get_invalid_rows(self):
        return self.df[self.df['failed_columns'].apply(len) > 0].copy()

    def get_all_rows(self):
        return self.df.copy()


# --- Validation functions ---

def is_clean_name(name):
    if pd.isna(name) or str(name).strip() == '':
        return False
    name_str = str(name)
    return all(c not in name_str for c in ['&', '"', "'", '/', ',']) and len(name_str.split()) == 1

def is_clean_street_number(value):
    try:
        return str(value).isdigit() and 1 <= int(value) <= 9999
    except (ValueError, TypeError):
        return False

def is_not_empty(value):
    if isinstance(value, str):
        return value.strip() != ''
    return pd.notnull(value)

def is_dog_or_cat(value):
    return str(value).strip().lower() in ['dog', 'cat']

def is_none(value):
    return str(value).strip().lower() != 'none'


# --- DAG task functions ---

def validate_cleaned_data():
    df = pd.read_csv('/opt/airflow/data/cleaned/clean_columns_client_data.csv')

    validator = ColumnValidator(df)

    # run validation functions on dataframe and log result to the record.
    validator.validate('first_name', is_clean_name)
    validator.validate('last_name', is_clean_name)
    validator.validate('street_number', is_clean_street_number)

    # Add more column-specific checks as needed
    columns_to_check = ['street_number', 'street_name', 'city', 'postal_code', 'owner_email','license_expiry']

    for col in columns_to_check:
        validator.validate(col, is_not_empty)

    validator.get_valid_rows().to_csv('/opt/airflow/data/validated/valid_records.csv', index=False)
    validator.get_invalid_rows().to_csv('/opt/airflow/data/validated/invalid_records.csv', index=False)
    validator.get_all_rows().to_csv('/opt/airflow/data/validated/all_records.csv', index=False)


def deduplicate_cleaned_data():
    df = pd.read_csv('/opt/airflow/data/validated/valid_records.csv')

    df['dedup_key'] = (
        df['first_name'].str.strip().str.lower() + '-' +
        df['last_name'].str.strip().str.lower() + '-' +
        df['owner_email'].str.strip().str.lower() +
        df['last_renewal_date'].astype(str).str.strip()
    )

    df = df.drop_duplicates(subset='dedup_key').drop(columns='dedup_key')
    
    df.to_csv('/opt/airflow/data/validated/final_valid_records.csv', index=False)



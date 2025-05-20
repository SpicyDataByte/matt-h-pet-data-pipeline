# 🐾 Pet Data Ingestion and Normalization Pipeline by Matt Hildebrandt

This project is a data pipeline built with Apache Airflow, Python and SQL for processing, validating, and loading pet license data into a PostgreSQL database. It uses a modular ETL (Extract, Transform, Load) structure and Dockerized environment to ensure reproducibility and scalability.

---

## 📂 Project Structure

```
.
├── dags/                      # Airflow DAG definition
├── etl/                       # ETL Python modules
├── data/                      # Source Excel data
├── init/                      # SQL file for initializing DB schema
├── Dockerfile                 # Custom Airflow Docker image
├── tests                      # transform_helpers tests
├── docker-compose.yml         # Docker orchestration
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

---

## 🚀 Quickstart. There is also a Notebook that walks through what pipeline does. 

### 1. Clone the repository

```bash
git clone https://github.com/SpicyDataByte/matt-h-pet-data-pipeline.git
cd matt-h-pet-data-pipeline
```

### 2. Build and start the services
#### Install docker: https://docs.docker.com/get-started/get-docker/

```bash
docker-compose up --build
```

> This will:
> - Build a custom Airflow image
> - Initialize the database
> - Start the Airflow webserver and scheduler
> - Seed the database schema via `init.sql`

### 3. Access the Airflow UI

- Navigate to [http://localhost:8080](http://localhost:8080)
- Login using:
  - **Username:** `admin`
  - **Password:** `admin`

### 4. Trigger the Pipeline

You can trigger the DAG using the Airflow UI or from the command line.

#### Option A: From the Airflow UI:
- Enable the DAG named `client_data_to_db_dag`
- Click “Trigger DAG” to run the full pipeline

#### Option B: From the command line:
```bash
docker exec -it airflow-webserver bash
airflow dags unpause client_data_to_db_dag && airflow dags trigger client_data_to_db_dag

airflow dags list-runs -d client_data_to_db_dag
```

---

## 🧱 Pipeline Stages

The Airflow DAG executes the following steps:

1. **Extract**
   - Reads `data.xlsx` and stages it.

2. **Transform**
   - Cleans and standardizes columns
   - Standardizes and infers province from phone
   - Categorizes city and province
   - Normalizes pet species and unit formatting
   - More to do but the pattern is there

3. **Validate**
   - Created a class to run validation rules on data
   - Bad data is flagged and logged with the record. 
   - Writes valid, invalid, and all records to separate files

4. **Deduplicate**
   - Uses a composite deduplication key to filter unique records

5. **Load**
   - Generates surrogate keys for owners and pets using SHA-256 hashing
   - Loads normalized dimension and fact tables to PostgreSQL

---

## 🗄️ Database Schema

- `pet_species`, `pet_breed`, `pet_colour`: Lookup tables
- `pet_owners`: Owner details with surrogate PK
- `pets`: Pets with references to owners and dimensions
- `license`: Licensing records referencing pets

- All constraints and cascade delete logic are defined in [`init/init.sql`](init/init.sql).
---
## Connect to Database:
 - host: localhost
 - port: 5432
 - username: airflow
 - password: airflow
 - database: pet_database
---

## ⚙️ Configuration

### Dockerfile

Defines:
- Airflow version (`2.9.1-python3.9`)
- Required folders and permissions
- Copies DAGs, ETL logic, and input data

### docker-compose.yml

Services:
- `postgres`: PostgreSQL 13 with pre-seeded schema
- `airflow-webserver`: Airflow UI + DAG execution
- `airflow-scheduler`: Background task scheduler

Volumes:
- `airflow-data`: Mount point for cleaned data
- `postgres-db-volume`: Persistent DB volume

---

## 🧪 Validation Logic

- `province`: If missing or invalid, the province is inferred using the area code from the owner's phone number. A known mapping of Canadian area codes to provinces is used for inference; otherwise, an "Add area code to dict" message is recorded for unknown area codes.
- `first_name` / `last_name`: Must not contain special characters (e.g., no `&`, `"`, `'`, `/`, or `,`)
- `street_number`: Must be a numeric string between 1 and 9999
- Required fields such as `street_name`, `city`, `postal_code`, `owner_email`, and `license_expiry` must not be empty. Used these as it made solution easier to follow. There was too much missing data that was flagged otherwise. 
- `pet_species`: Must be either "dog" or "cat" (case-insensitive, standardized from values like 'd' or 'c')

---

## 🔑 Surrogate Key Generation

Surrogate keys are generated using SHA-256 hash values based on deterministic column combinations. This ensures idempotent inserts and consistent keys over time.

---

## 📌 Notes

- Uses SHA-256 hash truncation for stable surrogate keys
- All logic is written in modular, testable Python functions
- Designed to be extended with more transformation or validation steps
- Tracks valid, invalid, and all records separately

--- 
## 📌 Data Model
```
pet_species       pet_breed         pet_colour
-------------     -------------     ---------------
species_id (PK)   breed_id (PK)     colour_id (PK)
species_name      breed_name        colour_name

            ↘         ↘                ↘
              ↘         ↘                ↘
               ┌────────────────────────────┐
   ------------│            pets            │
   |           └────────────────────────────┘
   |            pet_id (PK)
   |            pet_owner_id (FK → pet_owners)
   |            pet_name
   |            species_id (FK → pet_species)
   |            breed_id (FK → pet_breed)
   |            colour_id (FK → pet_colour)
   |            sex
   |            is_fixed
   |            pet_status
   |
   |                     ↓
   |          ┌────────────────────┐
   |          │     license        │
   |          └────────────────────┘
   |          license_id (PK)
   |          pet_id (FK → pets)
   |          license_tag_number
   |          last_date_issued
   |          license_expiry
   |          tag_status
   | 
   | 
   ↓
┌────────────────────────────┐
│        pet_owners          │
└────────────────────────────┘
     pet_owner_id (PK)
     owner_first_name
     owner_last_name
     owner_street_number
     owner_street_name
     owner_unit
     owner_city
     owner_province
     owner_postal_code
     owner_phone
     owner_email
     owner_alternate_phone
```
---
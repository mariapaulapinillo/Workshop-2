# Workshop-2
This project implements an ETL process (Extract, Transform, Load) on a spotify and grammy dataset, aiming to analyze the information and generate meaningful KPIs.
This project implements an **ETL (Extract, Transform, Load) pipeline** orchestrated with **Apache Airflow**, integrating two data sources:

- **Spotify Dataset** → Contains detailed information about songs, artists, popularity, streams, and musical attributes.  
- **Grammy Awards Dataset** → Historical records of Grammy nominees and winners.

The main goal is to **combine both datasets** to analyze the characteristics of award-winning songs and understand how popularity and genres have evolved over time.

## ⚙️ Pipeline Architecture

The workflow is orchestrated using **Docker Compose + Airflow**, with a single main **DAG** managing the entire ETL process:

etl_spotify_grammys
│
├── extract_spotify_csv → Extracts data from the Spotify CSV
├── extract_grammys_db → Extracts Grammy data from PostgreSQL 
├── transform_merge → Cleans, normalizes, and merges both datasets
├── load_to_gdrive_csv → Exports the final dataset to CSV 
└── load_to_dw → Loads the transformed dataset into the Data Warehouse 

## 🧩 Transformation Process

During the transformation stage (`transform_merge`), the following key steps were applied:

- Removed irrelevant or redundant columns.  
- Normalized text fields (lowercase, accent removal, no punctuation).  
- Split genre hierarchies into `main_genre` and `sub_genre`.  
- Applied **fuzzy string matching (RapidFuzz ≥90%)** to match artist and song title.  
- Added a `grammy_nominee = True` flag for matched Grammy entries.  
- Handled missing values using `fillna("")` or `NaN` depending on data type.

# Death Metal Data Warehouse 🤘

A modern data warehouse for analyzing death metal bands, albums, and reviews using DLT, dbt, and BigQuery/DuckDB.

## 📊 Data Source

- **Dataset**: [Death Metal Dataset on Kaggle](https://www.kaggle.com/datasets/zhangjuefei/death-metal)
- **Files**: `bands.csv`, `albums.csv`, `reviews.csv` (~15MB total)

## 🏗️ Architecture

```
Raw Data (S3/GCS) → DLT Pipeline → Data Warehouse (BigQuery/DuckDB) → dbt Transformations → Analytics (Superset)
```

### Components:
- **Ingestion**: DLT extracts CSV data from cloud storage
- **Transformation**: dbt models (staging → intermediate → marts)
- **Storage**: BigQuery (production) or DuckDB (local development)
- **Analytics**: Apache Superset for dashboards and visualization
- **Infrastructure**: Docker Compose for local services

### Data Models:
- **Staging**: Clean and standardize raw data
- **Intermediate**: Parse complex fields (band active periods, name changes)
- **Core Marts**: `dim_bands`, `dim_albums`, `fct_reviews`
- **Analytics**: Band metrics, geographic analysis, career patterns, temporal trends

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- Kaggle account
- Google Cloud Project (for BigQuery setup)

### 1. Clone and Install
```bash
git clone <repo-url>
cd deathmetal-dw
pip install -r requirements.txt
```

### 2. Download Data
1. Download from [Kaggle](https://www.kaggle.com/datasets/zhangjuefei/death-metal)
2. Extract: `bands.csv`, `albums.csv`, `reviews.csv`

### 3. Choose Your Setup

#### Option A: Local Development (DuckDB)
```bash
# Start local infrastructure
docker-compose up -d

# Upload data to MinIO (localhost:9001, minioadmin/minioadmin123)
# Go to "death-metal-raw" bucket and upload the 3 CSV files

# Configure public bucket for development
docker exec -it minio_death_metal mc anonymous set public local/death-metal-raw

# Run pipeline
export DBT_USE_BIGQUERY=false
python death_metal_pipeline.py

# Run dbt transformations
cd dbt_deathmetal
dbt run && dbt test
```

#### Option B: BigQuery Production

##### BigQuery Setup:
1. **Create Service Account**:
   ```bash
   # In Google Cloud Console
   # Go to IAM & Admin → Service Accounts → Create Service Account
   # Grant roles: BigQuery Admin, Storage Admin
   # Create and download JSON key file
   ```

2. **Create Storage Bucket**:
   ```bash
   gsutil mb gs://death-metal-raw-data
   gsutil cp bands.csv albums.csv reviews.csv gs://death-metal-raw-data/
   ```

3. **Configure Environment**:
   ```bash
   export DBT_USE_BIGQUERY=true
   export DBT_BIGQUERY_PROJECT=your-project-id
   export DBT_BIGQUERY_KEYFILE=/path/to/your-keyfile.json
   export DBT_BIGQUERY_LOCATION=US
   ```

4. **Run Pipeline**:
   ```bash
   python death_metal_pipeline.py
   cd dbt_deathmetal && dbt run && dbt test
   ```

### 4. Access Tools
- **Superset**: http://localhost:8089 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
- **Jupyter**: http://localhost:8888 (token: death-metal-jupyter-2024)

## 📊 Key Analytics

### Available Insights:
- **Band Metrics**: Discography size, average scores, quality consistency
- **Geographic Analysis**: Death metal production by country/continent
- **Career Analysis**: "Sophomore slump" patterns, career phases
- **Temporal Analysis**: Death metal evolution by decade
- **Subgenre Analysis**: Technical vs Brutal vs Melodic Death Metal

### Sample Queries:
```sql
-- Top bands by average score
SELECT band_name, total_albums, avg_score, pct_excellent
FROM band_metrics 
WHERE has_substantial_catalog = 1
ORDER BY avg_score DESC;

-- Geographic distribution
SELECT country, total_bands, dominant_subgenre
FROM geographic_analysis
ORDER BY total_bands DESC;

-- Career patterns
SELECT band_name, album_title, has_sophomore_slump
FROM career_analysis 
WHERE is_band_best_album = 1;
```

## 🔧 Configuration

### Environment Variables:
```bash
# BigQuery (Production)
export DBT_USE_BIGQUERY=true
export DBT_BIGQUERY_PROJECT=your-project-id
export DBT_BIGQUERY_KEYFILE=/path/to/keyfile.json

# DuckDB (Development) 
export DBT_USE_BIGQUERY=false
export DBT_DUCKDB_PATH=./death_metal.duckdb
export DBT_TARGET=dev
```

### File Structure:
```
├── .dlt/                    # DLT configurations
├── dbt_deathmetal/         # dbt project
│   ├── models/staging/     # Clean raw data
│   ├── models/intermediate/# Parse complex fields
│   └── models/marts/       # Final analytics models
├── death_metal_pipeline.py # DLT pipeline
├── docker-compose.yml      # Local infrastructure
└── requirements.txt        # Dependencies
```

## 🛠️ Technologies

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Ingestion** | DLT | Data pipeline automation |
| **Transformation** | dbt | SQL-based modeling |
| **Storage** | BigQuery/DuckDB | Data warehouse |
| **Analytics** | Apache Superset | BI dashboards |
| **Infrastructure** | Docker Compose | Local development |

## 🧪 Testing & Quality

```bash
# Run dbt tests
cd dbt_deathmetal
dbt test

# Generate documentation
dbt docs generate && dbt docs serve
```

Tests include data integrity, relationships, and custom business logic validation.

## 🔍 Troubleshooting

### Common Issues:

**MinIO Access Denied**:
```bash
docker exec -it minio_death_metal mc anonymous set public local/death-metal-raw
```

**BigQuery Authentication**:
```bash
# Verify service account has BigQuery Admin + Storage Admin roles
# Check keyfile path and project ID
```

**Data Not Appearing**:
```bash
# Verify CSV files are in correct bucket location
# Check DLT pipeline logs for errors
```

---

**Built with 🤘 for analyzing death metal data at scale**
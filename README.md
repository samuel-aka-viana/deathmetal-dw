# Death Metal Data Warehouse 🤘

A complete data warehouse project for analyzing death metal band data, built with modern data engineering technologies.

## 📊 Data Source

This project uses the **"Death Metal"** dataset available on Kaggle:
- **Dataset**: [Death Metal Dataset](https://www.kaggle.com/datasets/zhangjuefei/death-metal)
- **Files**: `bands.csv`, `albums.csv`, `reviews.csv`
- **Content**: Information about death metal bands, their albums and reviews
- **Size**: ~15MB total

> ⚠️ **Important**: You need to download the data from Kaggle and upload to MinIO before running the pipeline.

## 🏗️ Overall Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw Data      │    │     DLT      │    │     dbt         │    │   Analytics     │
│   (S3/MinIO)    │───▶│   Pipeline   │───▶│ Transformations │───▶│   (Superset)    │
│                 │    │              │    │                 │    │                 │
│ • bands.csv     │    │              │    │ • Staging       │    │ • Dashboards    │
│ • albums.csv    │    │              │    │ • Intermediate  │    │ • Reports       │
│ • reviews.csv   │    │              │    │ • Marts         │    │ • Analytics     │
└─────────────────┘    └──────────────┘    └─────────────────┘    └─────────────────┘
                              │                       │
                              ▼                       ▼
                       ┌──────────────┐        ┌─────────────────┐
                       │   DuckDB     │        │    Jupyter      │
                       │  (Database)  │        │   (Analysis)    │
                       └──────────────┘        └─────────────────┘
```

## 📋 Architecture Components

### 1. **Ingestion Layer - DLT Pipeline**
- **File**: `death_metal_pipeline.py`
- **Technology**: [DLT (Data Load Tool)](https://dlthub.com/)
- **Function**: Extracts data from CSVs in MinIO/S3 and loads into DuckDB
- **Resources**:
  - `metal_bands`: Band data
  - `metal_albums`: Album information
  - `metal_reviews`: Reviews and ratings

### 2. **Transformation Layer - dbt**
- **Directory**: `dbt_deathmetal/`
- **Technology**: [dbt (Data Build Tool)](https://www.getdbt.com/)
- **Structure**:

#### **Staging Models** (`models/staging/`)
```sql
stg_metal_bands     # Cleaning and standardization of band data
stg_metal_albums    # Initial processing of albums
stg_metal_reviews   # Standardization of reviews
```

#### **Intermediate Models** (`models/intermediate/`)
```sql
int_metal_bands_parsed  # Complex parsing of active periods,
                       # band status, name changes
```

#### **Marts - Core Dimensions** (`models/marts/core/`)
```sql
dim_bands    # Main band dimension with classifications
dim_albums   # Album dimension with career context
fct_reviews  # Central review fact with metrics
```

#### **Marts - Analytics** (`models/marts/`)
```sql
band_metrics         # Aggregated metrics by band
career_analysis      # Career pattern analysis
geographic_analysis  # Analysis by country/continent
subgenre_analysis   # Analysis by subgenre
temporal_analysis   # Temporal/evolutionary analysis
```

### 3. **Storage Layer - DuckDB**
- **File**: `death_metal.duckdb`
- **Technology**: [DuckDB](https://duckdb.org/)
- **Advantages**: 
  - OLAP optimized
  - Fast analytical queries
  - No server required
  - Native Python/dbt integration

### 4. **Analytics Layer - Superset**
- **Technology**: [Apache Superset](https://superset.apache.org/)
- **Port**: 8088
- **Function**: Dashboards, visualizations and reports
- **Default credentials**: admin/admin

### 5. **Infrastructure - Docker Compose**
- **MinIO**: S3-compatible storage (ports 9000/9001)
- **Superset**: BI Platform (port 8088)
- **Jupyter**: Ad-hoc analysis notebooks (port 8888)

## 🚀 How to Run

### Prerequisites
```bash
- Docker & Docker Compose
- Python 3.8+
- Git
- Kaggle account (for data download)
```

### 1. Initial Setup
```bash
# Clone the repository
git clone <repo-url>
cd deathmetal-dw

# Install Python dependencies
pip install -r requirements.txt

# Start infrastructure
docker-compose up -d
```

### 2. Configure Kaggle Data
```bash
# 1. Download dataset from Kaggle
# Access: https://www.kaggle.com/datasets/zhangjuefei/death-metal
# Download the death-metal.zip file

# 2. Extract CSV files
unzip death-metal.zip
# You should have: bands.csv, albums.csv, reviews.csv
```

### 3. Upload Data to MinIO
```bash
# Option 1: Via Web Interface (Recommended for development)
# 1. Access http://localhost:9001
# 2. Login: minioadmin / minioadmin123
# 3. Go to "death-metal-raw" bucket
# 4. Upload files: bands.csv, albums.csv, reviews.csv

# Option 2: Via MinIO Client (mc)
docker exec -it minio_death_metal mc alias set local http://localhost:9000 minioadmin minioadmin123
docker exec -it minio_death_metal mc cp /path/to/bands.csv local/death-metal-raw/
docker exec -it minio_death_metal mc cp /path/to/albums.csv local/death-metal-raw/
docker exec -it minio_death_metal mc cp /path/to/reviews.csv local/death-metal-raw/
```

### 4. Configure Public Bucket (Development)
```bash
# Via MinIO Console (Web Interface)
# 1. Access http://localhost:9001
# 2. Go to "Buckets" → "death-metal-raw"
# 3. Click "Manage" → "Access Rules"
# 4. Add rule: Prefix "*", Access "readonly" or "readwrite"

# Via MinIO Client (mc)
docker exec -it minio_death_metal mc anonymous set public local/death-metal-raw
# Or for read-only:
docker exec -it minio_death_metal mc anonymous set download local/death-metal-raw
```

### 5. Verify Data Upload
```bash
# List files in bucket
docker exec -it minio_death_metal mc ls local/death-metal-raw/

# Should show:
# bands.csv
# albums.csv  
# reviews.csv
```

### 6. Run DLT Pipeline
```bash
# Run ingestion pipeline
python death_metal_pipeline.py
```

### 7. Run dbt Transformations
```bash
cd dbt_deathmetal

# Install dbt dependencies
dbt deps

# Run transformations
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### 8. Access Tools
- **Superset**: http://localhost:8088 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
- **Jupyter**: http://localhost:8888 (token: death-metal-jupyter-2024)
- **dbt Docs**: http://localhost:8080

## 📊 Data Structure

### Main Entities

#### **Bands** (`dim_bands`)
- Basic information: name, country, status, formation year
- Classifications: continent, formation era, maturity
- Subgenres: Technical, Brutal, Melodic Death Metal, etc.
- Flags: active, veteran, name change

#### **Albums** (`dim_albums`)
- Album data: title, year, band
- Career context: band phase, discography number
- Temporal classifications: release era, decade
- Analysis: debut album, legacy, etc.

#### **Reviews** (`fct_reviews`)
- Reviews: score, title, content
- Rankings: overall position, by band, by country
- Metrics: percentiles, comparisons with averages
- Flags: excellent, above average, etc.

### Available Analysis

#### **Band Metrics**
```sql
SELECT 
  band_name,
  total_albums,
  avg_score,
  pct_excellent,
  years_active
FROM band_metrics
WHERE has_substantial_catalog = 1
ORDER BY avg_score DESC;
```

#### **Geographic Analysis**
```sql
SELECT 
  country,
  total_bands,
  albums_per_band,
  dominant_subgenre
FROM geographic_analysis
ORDER BY total_bands DESC;
```

#### **Career Analysis**
```sql
SELECT 
  band_name,
  album_title,
  album_career_position,
  score_vs_band_avg,
  has_sophomore_slump
FROM career_analysis
WHERE is_band_best_album = 1;
```

## 🎯 Key Insights the System Provides

### 1. **Career Analysis**
- "Sophomore slump" pattern (second album worse)
- Quality evolution over time
- Career phases vs album quality

### 2. **Geographic Analysis**
- Countries with highest death metal production
- Average quality by region
- Dominant subgenres by country

### 3. **Temporal Analysis**
- Death metal evolution by decade
- Golden Age (90s) vs modern era
- Quality trends over time

### 4. **Subgenre Analysis**
- Technical vs Brutal vs Melodic Death Metal
- Average quality by subgenre
- Geographic distribution of subgenres

## 🔧 Configurations

### DLT Configuration (`.dlt/config.toml`)
```toml
[pipeline.death_metal_pipeline]
destination = "duckdb"

[destination.duckdb]
dataset_name = "death_metal"

[sources.filesystem]
bucket_url = "s3://death-metal-raw"
```

### dbt Configuration (`dbt_project.yml`)
```yaml
name: 'dbt_deathmetal'
models:
  dbt_deathmetal:
    staging:
      +materialized: table
```

## 🧪 Testing and Quality

### Implemented dbt Tests
- **Integrity tests**: not_null, unique
- **Relationship tests**: foreign keys
- **Value tests**: accepted_values
- **Custom tests**: expression_is_true

### Run Tests
```bash
cd dbt_deathmetal
dbt test
```

## 📈 Future Extensions

### Possible Improvements
1. **Streaming**: Add Kafka for real-time data
2. **ML**: Band/album recommendation models
3. **API**: Data exposure via FastAPI
4. **Orchestration**: Add Airflow/Prefect
5. **Monitoring**: Observability with Grafana

### New Data Sources
- Last.fm API
- Spotify API
- Rate Your Music
- Metal Archives

## 🛠️ Technologies Used

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Ingestion** | DLT | Data pipeline |
| **Transformation** | dbt | Data modeling |
| **Storage** | DuckDB | Data warehouse |
| **Orchestration** | Docker Compose | Infrastructure |
| **BI** | Apache Superset | Dashboards |
| **Analysis** | Jupyter | Ad-hoc analysis |
| **Object Storage** | MinIO | File storage |

## 🛠️ Troubleshooting

### Common MinIO Issues

#### Error: "Bucket not found"
```bash
# Check if bucket was created
docker exec -it minio_death_metal mc ls local/

# Create bucket manually if needed
docker exec -it minio_death_metal mc mb local/death-metal-raw
```

#### Error: "Access Denied" in DLT Pipeline
```bash
# Check bucket permissions
docker exec -it minio_death_metal mc stat local/death-metal-raw

# Configure bucket as public (development)
docker exec -it minio_death_metal mc anonymous set public local/death-metal-raw
```

#### Verify CSV files are in correct location
```bash
# List bucket contents
docker exec -it minio_death_metal mc ls local/death-metal-raw/

# Should show:
# [DATE] [TIME]  xxxKB bands.csv
# [DATE] [TIME]  xxxKB albums.csv  
# [DATE] [TIME]  xxxKB reviews.csv
```

### DLT Issues

#### Pipeline fails with connection error
```bash
# Check if MinIO is running
docker ps | grep minio

# Test connectivity
curl http://localhost:9000/minio/health/live
```

#### Data doesn't appear in DuckDB
```bash
# Check if file was created
ls -la death_metal.duckdb

# Connect to DuckDB and check tables
python -c "import duckdb; conn = duckdb.connect('death_metal.duckdb'); print(conn.execute('SHOW TABLES').fetchall())"
```

### Development vs Production Configuration

#### For Development (Public Bucket)
```bash
# Fully public bucket - ONLY for development
docker exec -it minio_death_metal mc anonymous set public local/death-metal-raw
```

#### For Production (Private Bucket)
```bash
# Remove public access
docker exec -it minio_death_metal mc anonymous set none local/death-metal-raw

# Use specific credentials (already configured in .dlt/secrets.toml)
# aws_access_key_id = "admin"
# aws_secret_access_key = "password123"
```

### Quick Start for Development
```bash
# Complete setup in 5 minutes
git clone <repo-url> && cd deathmetal-dw
pip install -r requirements.txt
docker-compose up -d

# Wait for containers to start (30s)
sleep 30

# Download data from Kaggle manually and upload via web:
# 1. https://www.kaggle.com/datasets/zhangjuefei/death-metal
# 2. http://localhost:9001 (minioadmin/minioadmin123)
# 3. Upload to "death-metal-raw" bucket

# Configure public bucket
docker exec -it minio_death_metal mc anonymous set public local/death-metal-raw

# Run pipeline
python death_metal_pipeline.py

# Run dbt transformations
cd dbt_deathmetal && dbt run && dbt test
```

## 📝 Project Structure

```
deathmetal-dw/
├── .dlt/                          # DLT configurations
│   ├── config.toml               # Pipeline config
│   └── secrets.toml              # Credentials (git-ignored)
├── dbt_deathmetal/               # dbt project
│   ├── models/
│   │   ├── staging/              # Staging models
│   │   ├── intermediate/         # Intermediate models
│   │   └── marts/               # Final models
│   ├── tests/                   # Custom tests
│   └── dbt_project.yml          # dbt config
├── death_metal_pipeline.py       # DLT pipeline
├── docker-compose.yml            # Infrastructure
├── requirements.txt              # Python dependencies
└── README.md                     # Documentation
```

## 🤝 Contributing

1. Fork the project
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## 📄 License

This project is under the MIT license. See the LICENSE file for details.

---

**Built with 🤘 for the death metal community**
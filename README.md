# Death Metal Data Warehouse 🤘

[![dbt](https://img.shields.io/badge/dbt-1.10-orange)](https://docs.getdbt.com/)
[![DLT](https://img.shields.io/badge/DLT-1.11-blue)](https://dlthub.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Ready-green)](https://cloud.google.com/bigquery)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.3-yellow)](https://duckdb.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://docs.docker.com/compose/)

A modern data warehouse for analyzing death metal bands, albums, and reviews using **DLT**, **dbt**, and **BigQuery/DuckDB**. This project demonstrates a complete ELT pipeline with data modeling, quality testing, and analytics capabilities.

## 📊 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Data Sources                              │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐     │
│   │   bands.csv │  │ albums.csv  │  │      reviews.csv        │     │
│   │   (~5MB)    │  │   (~3MB)    │  │        (~7MB)           │     │
│   └─────────────┘  └─────────────┘  └─────────────────────────┘     │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Storage Layer                                  │
│   ┌─────────────┐           ┌─────────────────────────────────┐     │
│   │    MinIO    │           │         Google Cloud           │      │
│   │ (S3 Local)  │    OR     │      Storage Bucket             │     │
│   │ Port: 9001  │           │     (gs://bucket-name)          │     │
│   └─────────────┘           └─────────────────────────────────┘     │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Ingestion Layer (DLT)                            │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │  death_metal_pipeline.py                                    │   │
│   │  • Extract CSV files from cloud storage                     │   │
│   │  • Load into warehouse with proper schema                   │   │
│   │  • Cross-platform (BigQuery/DuckDB)                         │   │
│   └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Data Warehouse                                   │
│   ┌─────────────┐           ┌─────────────────────────────────┐     │
│   │   DuckDB    │           │         BigQuery                │     │
│   │ (Local Dev) │    OR     │       (Production)              │     │
│   │ File-based  │           │   Serverless & Scalable         │     │
│   └─────────────┘           └─────────────────────────────────┘     │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 Transformation Layer (dbt)                          │
│   ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐        │
│   │   Staging   │  │ Intermediate │  │       Marts         │        │
│   │ • Clean raw │  │ • Parse      │  │ • dim_bands         │        │
│   │ • Validate  │  │ • Enrich     │  │ • dim_albums        │        │
│   │ • Normalize │  │ • Transform  │  │ • fct_reviews       │        │
│   └─────────────┘  └──────────────┘  └─────────────────────┘        │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Analytics Layer                                  │
│   ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐        │
│   │  Superset   │  │   Jupyter    │  │    dbt docs         │        │
│   │ Dashboards  │  │  Notebooks   │  │  Documentation      │        │
│   │ Port: 8089  │  │ Port: 8888   │  │  Port: 8080         │        │
│   └─────────────┘  └──────────────┘  └─────────────────────┘        │
└─────────────────────────────────────────────────────────────────────┘
```

## 🛠️ Technology Stack

| Component | Technology | Purpose | Port |
|-----------|------------|---------|------|
| **Ingestion** | DLT | Data pipeline automation | - |
| **Transformation** | dbt | SQL-based modeling | - |
| **Storage (Dev)** | DuckDB | Local data warehouse | - |
| **Storage (Prod)** | BigQuery | Cloud data warehouse | - |
| **Object Storage** | MinIO | S3-compatible storage | 9000, 9001 |
| **Analytics** | Apache Superset | BI dashboards | 8089 |
| **Notebooks** | Jupyter | Data exploration | 8888 |
| **Orchestration** | Prefect | Workflow management | - |

## 🚀 Quick Start

### Prerequisites

- **Docker & Docker Compose** 20.10+
- **Python** 3.8+
- **8GB+ RAM** recommended
- **Google Cloud Project** (for BigQuery setup)
- **Kaggle account** (for data download)

### Option 1: Local Development (DuckDB)

```bash
# 1. Clone repository
git clone <your-repo-url>
cd death-metal-dw

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start infrastructure
docker-compose up -d

# 4. Download data from Kaggle
# Visit: https://www.kaggle.com/datasets/zhangjuefei/death-metal
# Download: bands.csv, albums.csv, reviews.csv

# 5. Upload to MinIO
# Access: http://localhost:9001 (minioadmin/minioadmin123)
# Upload files to "death-metal-raw" bucket

# 6. Configure environment
export DBT_USE_BIGQUERY=false
export DBT_DUCKDB_PATH=./death_metal.duckdb
export DBT_TARGET=dev

# 7. Run pipeline
python death_metal_pipeline.py

# 8. Run transformations
cd dbt_deathmetal
dbt run && dbt test
```

### Option 2: BigQuery Production

```bash
# 1. Setup Google Cloud
# Create service account with BigQuery Admin + Storage Admin roles
# Download JSON key file

# 2. Create storage bucket
gsutil mb gs://death-metal-raw-data
gsutil cp *.csv gs://death-metal-raw-data/

# 3. Configure environment
export DBT_USE_BIGQUERY=true
export DBT_BIGQUERY_PROJECT=your-project-id
export DBT_BIGQUERY_KEYFILE=/path/to/keyfile.json
export DBT_BIGQUERY_LOCATION=US
export DBT_TARGET=prod

# 4. Run pipeline
python death_metal_pipeline.py
cd dbt_deathmetal && dbt run && dbt test
```

## 📊 Data Models

### Core Dimensions

| Model | Description | Key Metrics |
|-------|-------------|-------------|
| **dim_bands** | Band master data with geographical and genre classification | 15,000+ bands, 50+ countries |
| **dim_albums** | Album catalog with career phase analysis | 45,000+ albums, 1980-2024 |

### Facts & Analytics

| Model | Description | Key Insights |
|-------|-------------|--------------|
| **fct_reviews** | Album reviews with comprehensive scoring metrics | 85,000+ reviews, quality distributions |
| **band_metrics** | Aggregated band performance and catalog analysis | Discography patterns, consistency scores |
| **geographic_analysis** | Death metal production by country/region | Global distribution, regional preferences |
| **career_analysis** | Band lifecycle and album progression patterns | Sophomore slump, career peaks |
| **temporal_analysis** | Historical trends and decade comparisons | Genre evolution, quality trends |
| **subgenre_analysis** | Death metal subgenre characteristics | Technical vs Brutal vs Melodic |

## 🔍 Key Analytics

### Sample Queries

**Top Rated Bands with Substantial Catalogs:**
```sql
SELECT band_name, total_albums, avg_score, pct_excellent
FROM band_metrics 
WHERE has_substantial_catalog = 1
ORDER BY avg_score DESC
LIMIT 10;
```

**Geographic Death Metal Distribution:**
```sql
SELECT country, total_bands, dominant_subgenre, avg_score
FROM geographic_analysis
WHERE total_bands >= 10
ORDER BY total_bands DESC;
```

**Career Pattern Analysis:**
```sql
SELECT 
    album_career_position,
    AVG(score_album) as avg_score,
    COUNT(*) as total_albums
FROM career_analysis 
GROUP BY album_career_position
ORDER BY avg_score DESC;
```

**Temporal Trends:**
```sql
SELECT 
    release_decade,
    albums_released,
    avg_score,
    dominant_subgenre
FROM temporal_analysis
ORDER BY release_decade;
```

## 🐳 Services & Access

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Superset** | [http://localhost:8089](http://localhost:8089) | admin/admin | BI Dashboards |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | minioadmin/minioadmin123 | Object Storage |
| **Jupyter** | [http://localhost:8888](http://localhost:8888) | Token: death-metal-jupyter-2024 | Data Exploration |
| **dbt docs** | [http://localhost:8080](http://localhost:8080) | - | Model Documentation |

## 🏗️ Project Structure

```
death-metal-dw/
├── .dlt/                          # DLT configurations
│   ├── config.toml               # Pipeline configuration
│   ├── config_bigquery.toml      # BigQuery-specific config
│   ├── secrets.toml              # Local credentials
│   └── secrets_bigquery.toml     # BigQuery credentials
├── dbt_deathmetal/               # dbt project
│   ├── models/
│   │   ├── staging/              # Raw data cleaning
│   │   ├── intermediate/         # Business logic parsing
│   │   └── marts/                # Final analytics models
│   ├── macros/                   # Cross-platform SQL functions
│   ├── tests/                    # Data quality tests
│   └── dbt_project.yml          # dbt configuration
├── workflows/                    # Prefect orchestration
│   ├── death_metal_flow.py      # Main pipeline flow
│   └── modern_dbt_pipeline.py   # dbt transformation flow
├── death_metal_pipeline.py      # DLT extraction script
├── docker-compose.yml           # Local infrastructure
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DBT_USE_BIGQUERY` | Use BigQuery (true) or DuckDB (false) | false | No |
| `DBT_TARGET` | dbt target environment | dev | No |
| `DBT_BIGQUERY_PROJECT` | Google Cloud project ID | - | BigQuery only |
| `DBT_BIGQUERY_KEYFILE` | Path to service account JSON | - | BigQuery only |
| `DBT_DUCKDB_PATH` | DuckDB database file path | ./death_metal.duckdb | DuckDB only |

### Data Source

- **Dataset**: [Death Metal Dataset on Kaggle](https://www.kaggle.com/datasets/zhangjuefei/death-metal)
- **Size**: ~15MB total (3 CSV files)
- **Records**: 15K+ bands, 45K+ albums, 85K+ reviews
- **Coverage**: Global death metal scene (1980-2024)

## 🧪 Testing & Quality

```bash
# Run all tests
cd dbt_deathmetal
dbt test

# Test specific models
dbt test --select dim_bands
dbt test --select fct_reviews

# Generate documentation
dbt docs generate
dbt docs serve
```

### Test Coverage

- **Schema Tests**: Not null, unique, relationships
- **Data Quality**: Value ranges, distributions
- **Business Logic**: Custom band metrics validation
- **Referential Integrity**: Cross-model relationships

## 🔧 Troubleshooting

### Common Issues

**MinIO Access Denied:**
```bash
docker exec -it minio_death_metal mc anonymous set public local/death-metal-raw
```

**BigQuery Authentication:**
```bash
# Verify service account permissions
gcloud auth activate-service-account --key-file=/path/to/keyfile.json
gcloud projects list
```

**Data Not Loading:**
```bash
# Check pipeline logs
python death_metal_pipeline.py

# Verify file upload
docker logs minio_death_metal
```

**dbt Connection Issues:**
```bash
# Test connection
cd dbt_deathmetal
dbt debug

# Check profiles
cat ~/.dbt/profiles.yml
```

## 📈 Performance & Resources

### System Requirements

- **Memory**: 8GB+ RAM (16GB recommended)
- **Storage**: 20GB+ available space
- **CPU**: 4+ cores recommended
- **Network**: Stable internet for BigQuery/GCS

### Resource Allocation

| Service | CPU | Memory | Purpose |
|---------|-----|--------|---------|
| MinIO | 1 CPU | 1GB | Object storage |
| Superset | 1 CPU | 2GB | BI platform |
| Jupyter | 1 CPU | 2GB | Data exploration |
| DuckDB | - | 2GB | Local processing |

## 🚀 Advanced Usage

### Prefect Orchestration

```bash
# Deploy workflows
prefect deploy --all

# Run specific flow
prefect deployment run "Death Metal Data Pipeline/death-metal-pipeline"
```

### Custom Analytics

```sql
-- Band Evolution Analysis
WITH band_timeline AS (
  SELECT 
    band_id,
    album_year,
    score_album,
    LAG(score_album) OVER (PARTITION BY band_id ORDER BY album_year) as prev_score
  FROM fct_reviews
)
SELECT 
  band_name,
  COUNT(CASE WHEN score_album > prev_score THEN 1 END) as improvements,
  COUNT(CASE WHEN score_album < prev_score THEN 1 END) as declines
FROM band_timeline bt
JOIN dim_bands b ON bt.band_id = b.band_id
GROUP BY band_name
HAVING COUNT(*) >= 5;
```

## 📚 Documentation

- **dbt docs**: Complete model documentation with lineage
- **Architecture**: Detailed system design and data flow
- **API Reference**: All available models and metrics
- **Business Logic**: Death metal domain knowledge

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Add tests for new functionality
4. Commit changes (`git commit -m 'Add amazing feature'`)
5. Push to branch (`git push origin feature/amazing-feature`)
6. Open Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Resources

### Technology Documentation
- [dbt Documentation](https://docs.getdbt.com/) - Modern data transformation
- [DLT Documentation](https://dlthub.com/docs/) - Data loading tool
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs) - Google's data warehouse
- [DuckDB Documentation](https://duckdb.org/docs/) - In-process SQL OLAP database

### Domain Knowledge
- [Death Metal Archives](https://www.metal-archives.com/) - Comprehensive metal database
- [Death Metal Evolution](https://en.wikipedia.org/wiki/Death_metal) - Genre history and characteristics

---

**Built with 🤘 for the global death metal community**

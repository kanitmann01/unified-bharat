# 🏛️ Unified Bharat: Cross-Sector Policy Analytics Lakehouse

> **Medallion Architecture for District-Level CSR, Groundwater & Institutional Data Integration**

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark 3.5](https://img.shields.io/badge/Spark-3.5-orange.svg)](https://spark.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Iceberg-1.4.3-teal.svg)](https://iceberg.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://docker.com/)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Medallion Pipeline](#medallion-pipeline)
  - [Bronze Layer](#bronze-layer-raw-ingestion)
  - [Silver Layer](#silver-layer-cleaning--standardization)
  - [Gold Layer](#gold-layer-unified-panel)
- [Data Quality & Lineage](#data-quality--lineage)
- [Analytics & Panel Regression](#analytics--panel-regression)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Key Results](#key-results)

---

## Overview

**Unified Bharat** is a distributed Lakehouse architecture that integrates cross-ministry datasets at the district-year level to analyze the relationship between:

- 💰 **Corporate Social Responsibility (CSR)** spending
- 💧 **Groundwater quality** indicators
- 🎓 **Educational institutions** metrics

### Research Question

> *Does subsequent improvement in groundwater quality associate with district-level CSR spending?*

The system conducts rigorous panel analysis while handling district and year fixed effects using a standard two-way fixed effects regression model.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    UNIFIED BHARAT LAKEHOUSE                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│   │   BRONZE     │───▶│   SILVER     │───▶│    GOLD      │ │
│   │  (Raw Data)  │    │ (Clean Data) │    │(Unified Panel│ │
│   └──────────────┘    └──────────────┘    └──────────────┘ │
│          │                   │                   │          │
│          ▼                   ▼                   ▼          │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│   │  245K rows   │    │  ~1,500 rows │    │  ~300 rows   │ │
│   │   5 tables   │    │   4 tables   │    │   1 panel    │ │
│   └──────────────┘    └──────────────┘    └──────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│                   ANALYTICS LAYER                            │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐ │
│  │   EDA      │  │ Clustering │  │ Panel Regression       │ │
│  │  Trends    │  │  K-Means   │  │ OLS | RF | XGBoost     │ │
│  │  Maps      │  │            │  │ Fixed Effects          │ │
│  └────────────┘  └────────────┘  └────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**Technology Stack:**
- **Apache Spark 3.5** + **PySpark** for distributed processing
- **Apache Iceberg** for lakehouse table format
- **MinIO** for S3-compatible object storage
- **Docker Compose** for local infrastructure
- **Python** ecosystem: pandas, scikit-learn, xgboost, statsmodels, geopandas

---

## Data Sources

| Dataset | Ministry | Granularity | Rows (Bronze) | Key Metrics |
|---------|----------|-------------|---------------|-------------|
| **CSR Spending** | Corporate Affairs | District-Year | 28,834 | INR Crores, USD Millions, Development Sectors |
| **Groundwater Quality** | Jal Shakti | Station-Level | 188,209 | Chemical params (Hardness, PH, Nitrate, Fluoride) |
| **Institutions** | Education | State-Institution-Year | 2,141 | Approved Intake, Institution Counts |
| **LGD Master** | Panchayati Raj | District Reference | 785 | Canonical District Names & Codes |
| **Population** | Census 2024 Est. | State-Level | 36 | Population Estimates (2024) |

---

## Medallion Pipeline

### Bronze Layer: Raw Ingestion

**Input:** 5 raw CSV files → **Iceberg Tables**

```python
local.bronze.csr_spending      # 28,834 rows
local.bronze.ground_water      # 188,209 rows  
local.bronze.institutions      # 2,141 rows
local.bronze.lgd_master        # 785 rows
local.bronze.population        # 36 rows
```

**Data Quality:** Baseline — raw data with original schema, nulls, and encoding issues preserved.

---

### Silver Layer: Cleaning & Standardization

#### 💰 CSR → `local.silver.csr_state_year`

```
Input:  28,834 rows (district-year)
Output:    ~400 rows (state-year)
Retention:  ~1.4%
```

**Transformations:**
| Step | Description | Data Quality Impact |
|------|-------------|---------------------|
| 1 | Extract year from "Financial Year (Apr - Mar), 20XX" | High — deterministic regex |
| 2 | Standardize state names to UPPERCASE | High — exact text transform |
| 3 | Convert INR crores → USD millions (rate: 83.0) | High — fixed conversion |
| 4 | **AVERAGE** across districts/departments | High — no estimation |

**Key Columns:** `state_name`, `year`, `avg_csr_inr_crores`, `avg_csr_usd_millions`

---

#### 💧 Groundwater → `local.silver.groundwater_state_year`

```
Input:  188,209 rows (station-level, 29 columns)
Output:    ~800 rows (state-year, 7 columns)
Retention:  ~0.4%
```

**Critical Decision — Column Filtering by Data Coverage:**

| Parameter | Data % | Decision | Rationale |
|-----------|--------|----------|-----------|
| **Hardness** | 69.18% | ✅ **KEEP** | Above 65% threshold |
| **PH_avg** | 74.54% | ✅ **KEEP** | Above 65% threshold |
| **Nitrate_avg** | 65.40% | ✅ **KEEP** | Above 65% threshold |
| **Fluoride_avg** | 71.49% | ✅ **KEEP** | Above 65% threshold |
| ~~TDS_avg~~ | ~~21.09%~~ | ❌ **DROP** | Too sparse |
| ~~Iron_avg~~ | ~~34.18%~~ | ❌ **DROP** | Too sparse |
| ~~Arsenic_avg~~ | ~~9.81%~~ | ❌ **DROP** | Too sparse |

**Transformations:**
1. Station-level → State-year aggregation (mean)
2. District fuzzy-matching to LGD master codes
3. Contamination index calculation (0-4 scale):
   - +1 if Hardness > 200 mg/L
   - +1 if PH < 6.5 or > 8.5
   - +1 if Nitrate > 45 mg/L
   - +1 if Fluoride > 1.5 mg/L

**Confidence:** Medium-High — LGD match rate tracked, only high-coverage params used.

---

#### 🎓 Institutions → `local.silver.institutions_state_year`

```
Input:   2,141 rows (state-institution-type-year)
Output:    ~350 rows (state-year)
Retention:  ~16%
```

**Transformations:**
| Step | Null Handling | Impact |
|------|--------------|--------|
| Extract year from text | — | Clean temporal dimension |
| Fill numeric nulls | `approved_intake` → 0 | 23 rows imputed |
| Fill numeric nulls | `institutions` → 0 | 45 rows imputed |
| Fill categoricals | `type` → "Unknown" | 12 rows imputed |
| **AVERAGE** across types | — | Collapse dimensionality |

**Key Columns:** `avg_approved_intake`, `avg_institutions`, `avg_total_approved_institutions`

---

### Gold Layer: Unified Panel

#### 💧 Water Quality Final Clean → `local.gold.groundwater_state_year`

```
Input:   ~800 rows (silver)
Output:  ~800 rows (gold)
Retention: 100% (column drop + fill)
```

**Transformations:**
1. Drop `match_confidence_pct` (tracked in lineage, not needed for analysis)
2. Fill remaining nulls in 4 chemical params with **column averages**
3. **Recalculate contamination_index** after fill (0-4 scale)

**Data Quality:** Very High — no nulls remain, only reliable parameters.

---

#### 🏆 Unified Panel → `local.gold.state_year_panel`

```
Input:  Silver tables (~1,550 total rows)
Output:    ~300 rows (unified panel)
Join:   Full outer join on (state_name, year) + Population left join
```

**Derived Metrics:**
| Metric | Formula | Purpose |
|--------|---------|---------|
| `csr_spent_lag1` | LAG(avg_csr_inr_crores, 1) | Temporal analysis |
| `csr_per_capita_inr` | (CSR × 10⁷) / Population | Normalized spending |
| `institutions_per_million` | (Institutions × 10⁶) / Population | Normalized capacity |
| `panel_completeness` | Count of datasets present (0-3) | Data quality flag |

**Panel Completeness:**
```
Complete (3/3 datasets):  ████████████  ~35%
Partial  (2/3 datasets):  ██████████████████  ~50%
Sparse   (1/3 datasets):  ██████  ~12%
Empty    (0/3 datasets):  █  ~3%
```

---

## Data Quality & Lineage

### Lineage Tracking

Every transformation is tracked automatically:

| Layer | Table | Rows In | Rows Out | % Retained | Confidence |
|-------|-------|---------|----------|------------|------------|
| Bronze | `csr_spending` | 28,834 | 28,834 | 100% | Baseline |
| Silver | `csr_state_year` | 28,834 | ~400 | 1.4% | High |
| Bronze | `ground_water` | 188,209 | 188,209 | 100% | Baseline |
| Silver | `groundwater_state_year` | 188,209 | ~800 | 0.4% | Medium-High |
| Gold | `groundwater_state_year` | ~800 | ~800 | 100% | Very High |
| Bronze | `institutions` | 2,141 | 2,141 | 100% | Baseline |
| Silver | `institutions_state_year` | 2,141 | ~350 | 16% | High |
| Gold | `state_year_panel` | ~1,550 | ~300 | ~20% | Panel-dependent |

**Auto-generated files:**
- `data/lineage_summary.csv` — Machine-readable tracking
- `work/DATA_LINEAGE.md` — Human-readable report updated on each run

---

## Analytics & Panel Regression

### 1. Exploratory Analysis

**Temporal Trends:**
- CSR spending trajectory across years
- Groundwater contamination index evolution
- Institutional capacity growth
- Per-capita normalized metrics

**Spatial Variation:**
- Top 15 states by contamination index
- Top 15 states by CSR per capita
- Top 15 states by institutions per million

**Correlation Heatmap:** Multi-sector indicator relationships

### 2. Clustering Analysis

**K-Means Clustering** (k=4) on state-level averages:

| Cluster | States | Profile |
|---------|--------|---------|
| **0** | Small states | Low CSR, Low contamination, Low institutions |
| **1** | Maharashtra | Very high CSR, High institutions |
| **2** | Tech hubs | High institutions, Moderate CSR |
| **3** | Large states | Moderate CSR, Higher contamination |

### 3. Panel Regression Models

**Specification:**
```
contamination_index_it = β₀ + β₁·csr_spent_lag1_it + α_i + γ_t + ε_it
```

Where:
- `α_i` = State fixed effects
- `γ_t` = Year fixed effects
- `csr_spent_lag1` = Lagged CSR spending (t-1)

**Models Compared:**

| Model | R² (In-Sample) | RMSE (In-Sample) | RMSE (5-Fold CV) |
|-------|----------------|------------------|------------------|
| **OLS** | 0.245 | 0.818 | — |
| **Random Forest** | 0.759 | 0.456 | 0.811 |
| **XGBoost** | 0.940 | 0.229 | 0.889 |

**Key Insight:** Random Forest achieves best generalization (lowest CV error), suggesting non-linear relationships between CSR and environmental outcomes.

---

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Step 1: Start Infrastructure

```bash
# From project root
docker-compose up -d

# Verify services
docker ps
```

| Service | Container | Ports |
|---------|-----------|-------|
| MinIO (S3) | `minio_storage` | 9000 (API), 9001 (Console) |
| Spark + Jupyter | `spark_compute` | 8888 (JupyterLab), 4040 (Spark UI) |

### Step 2: Configure MinIO

1. Open http://localhost:9001
2. Login: `admin` / `supersecretpassword`
3. Create bucket: `unified-bharat`

### Step 3: Get Jupyter URL

```bash
docker logs spark_compute
# Look for: http://127.0.0.1:8888/lab?token=...
```

### Step 4: Run Pipeline

1. Open JupyterLab URL in browser
2. Navigate to `work/` folder
3. **Run all cells** in `medallion.ipynb`
4. **Run all cells** in `analytics.ipynb`

### Step 5: View Outputs

```
work/
├── temporal_trends.png
├── correlation_heatmap.png
├── spatial_analysis.png
├── clustering_analysis.png
├── rf_feature_importance.png
├── xgb_feature_importance.png
└── DATA_LINEAGE.md (auto-updated)

data/gold/
├── gold_state_year_panel.csv
└── model_comparison.csv
```

### Stop Infrastructure

```bash
docker-compose down

# To remove data volumes:
docker-compose down -v
```

---

## Project Structure

```
unified-bharat/
├── 📁 notebooks/
│   ├── medallion.ipynb          # Bronze → Silver → Gold pipeline
│   └── analytics.ipynb          # EDA, clustering, regression
├── 📁 data/
│   ├── raw/                     # Bronze source files
│   │   ├── csr_district.csv
│   │   ├── ground_water.csv
│   │   ├── institutions.csv
│   │   ├── LGD.csv
│   │   └── population.csv
│   └── gold/                    # Gold outputs
│       ├── gold_state_year_panel.csv
│       └── model_comparison.csv
├── 📁 test/                     # NDAP data fetcher
│   ├── ndap/
│   │   ├── fetch_all.py
│   │   ├── ndap_client.py
│   │   └── sources.yaml
│   └── ndap_data/
├── docker-compose.yaml          # Infrastructure stack
├── requirements.txt             # Python dependencies
├── DATA_LINEAGE.md              # Auto-generated quality report
└── README.md                    # This file
```

---

## Key Results

### Data Reduction Summary

```
Bronze Total:     245,965 rows  (5 tables, raw)
    ↓
Silver Total:      ~1,550 rows  (4 tables, cleaned)
    ↓
Gold Panel:          ~300 rows  (1 table, unified)

Overall Reduction:  99.88% of rows removed
                    (Intentional — aggregation & quality filtering)
```

### Quality Improvements

| Metric | Before (Bronze) | After (Gold) | Improvement |
|--------|----------------|--------------|-------------|
| Null Rate (Water) | ~35% | 0% | Filled with column averages |
| Column Count (Water) | 29 | 6 | Removed low-coverage params |
| Spatial Granularity | District/Station | State | Properly aggregated |
| Temporal Granularity | Mixed formats | Integer year | Standardized |
| Currency | INR only | INR + USD | Dual reporting |

### Feature Importance (Random Forest)

```
num_stations          ████████████████████████████████████  51.0%
csr_spent_lag1        ████████████████                      21.0%
avg_institutions      ██████████████                        18.8%
institutions_per_mil   ██████                                9.3%
```

---

## References

- NITI Aayog. (n.d.). National Data & Analytics Platform (NDAP). https://ndap.niti.gov.in/
- Wooldridge, J. M. (2010). *Econometric analysis of cross section and panel data* (2nd ed.). MIT Press.
- Bureau of Indian Standards (BIS) for groundwater quality limits

---

## License

This project is for academic research purposes (INFO 584 — Team R2K).

---

<p align="center">
  <strong>Built with Apache Spark, Iceberg, and PySpark</strong><br>
  <em>Empowering data-driven policy insights for Bharat</em> 🇮🇳
</p>

# Unified Bharat: Data Lineage & Quality Report

**Auto-generated upon pipeline execution**

## Architecture Overview

```
Bronze Layer (Raw Ingestion)
├── local.bronze.csr_spending          28,834 rows
├── local.bronze.ground_water         188,209 rows
├── local.bronze.institutions           2,141 rows
├── local.bronze.lgd_master               785 rows
└── local.bronze.population                36 rows

Silver Layer (State-Year Processing)
├── local.silver.csr_state_year         ~400 rows  [AVERAGE aggregation]
├── local.silver.groundwater_state_year ~800 rows  [High-coverage params only]
├── local.silver.institutions_state_year ~350 rows [AVERAGE aggregation]
└── local.silver.population_state          36 rows

Gold Layer (Final Clean)
├── local.gold.groundwater_state_year   ~800 rows  [Nulls filled, index recalc]
└── local.gold.state_year_panel        ~200-300 rows [Unified panel]
```

## Data Quality Decisions

### Groundwater (Water Quality)

**Column Coverage Analysis:**

| Parameter | Data % | Decision |
|-----------|--------|----------|
| Hardness | 69.18% | **KEEP** |
| PH_avg | 74.54% | **KEEP** |
| Nitrate_avg | 65.40% | **KEEP** |
| Fluoride_avg | 71.49% | **KEEP** |
| TDS_avg | 21.09% | **DROP** |
| Iron_avg | 34.18% | **DROP** |
| Arsenic_avg | 8.81% | **DROP** |

**Transformations:**
1. Bronze (188K station rows) → Silver (~800 state-year rows)
2. Drop 3 low-coverage columns: TDS, Iron, Arsenic
3. Keep 4 high-coverage columns: Hardness, PH, Nitrate, Fluoride
4. Match districts to LGD master (confidence tracked)
5. Compute contamination_index on 0-4 scale (was 0-7)

**Gold Enhancement:**
1. Drop match_confidence_pct column
2. Fill nulls in 4 kept params with column averages
3. **Recalculate contamination_index** after null fill
4. Result: Very High confidence (no nulls, only reliable params)

### CSR Spending

**Transformations:**
1. Extract year from "Financial Year (Apr - Mar), 20XX"
2. Rename StateName → state_name
3. Convert INR crores → USD millions (rate: 83.0)
4. **AVERAGE** (not sum) across districts/departments
5. Result: ~400 state-year rows

### Institutions

**Transformations:**
1. Extract year from text
2. Rename columns to snake_case
3. Fill nulls: numeric=0, categorical='Unknown'
4. **AVERAGE** (not sum) across institution types
5. Result: ~350 state-year rows

### Gold Panel

**Join:** CSR + Groundwater + Institutions + Population
**Derived Metrics:**
- csr_spent_lag1: Previous year CSR (lag variable)
- csr_per_capita_inr: CSR / population
- institutions_per_million: Institutions / population
- panel_completeness: 0-3 (datasets present)

**Completeness Levels:**
- Complete (3/3): ~30-40%
- Partial (2/3): ~40-50%
- Sparse (1/3): ~10-20%
- Empty (0/3): <5%

## Files Generated

| File | Description |
|------|-------------|
| `data/gold/gold_state_year_panel.csv` | Unified panel for analysis |
| `data/lineage_summary.csv` | Machine-readable lineage tracking |
| `notebooks/medallion.ipynb` | Pipeline execution notebook |
| `notebooks/analytics.ipynb` | Analysis & regression notebook |
| `work/DATA_LINEAGE.md` | Auto-updated upon each run |

---

*Run medallion.ipynb to populate actual row counts and percentages.*

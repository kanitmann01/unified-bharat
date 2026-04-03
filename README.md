# Unified Bharat NDAP Fetcher

Python scripts and config to pull public datasets from India's NDAP Open API
(National Data and Analytics Platform), then save merged output under
`test/ndap_data/` as JSON and CSV.

## What this does

- Reads enabled sources from `test/ndap/sources.yaml`
- Calls NDAP Open API page by page
- Merges rows from all fetched pages into a single `records` list
- Writes one JSON snapshot per source
- Writes one CSV per source by flattening nested fields (for easy Excel use)

## Project structure

- `test/ndap/fetch_all.py` - CLI entry point and pagination logic
- `test/ndap/ndap_client.py` - HTTP client with retry handling
- `test/ndap/sources.yaml` - source list and query settings
- `test/ndap_data/` - generated JSON and CSV outputs
- `.env` - local secrets/config (not committed)
- `.env.example` - template for required environment variables
- `docker-compose.yaml` - local Spark, JupyterLab, and MinIO stack (S3-compatible storage)

## Docker Compose (Spark, MinIO, JupyterLab)

The stack is defined in [`docker-compose.yaml`](docker-compose.yaml). It pulls official images (`minio/minio`, `jupyter/all-spark-notebook`); there is no project-specific `Dockerfile`.

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed
- [Docker Compose](https://docs.docker.com/compose/install/) (v2 is bundled with Docker Desktop as `docker compose`)

### Step 1: Start the stack

From the repository root, in Bash:

```bash
docker-compose up -d
```

On newer setups you can use:

```bash
docker compose up -d
```

Docker will download the images (first run only) and start the services in the background.

| Service | Container name | Host ports |
|--------|-----------------|------------|
| MinIO (S3 API + console) | `minio_storage` | `9000` (API), `9001` (console) |
| JupyterLab + Spark | `spark_compute` | `8888` (JupyterLab), `4040` (Spark UI) |

### Step 2: Configure the storage lake (MinIO)

Before Spark can write data, create the object storage bucket.

1. Open a browser and go to [http://localhost:9001](http://localhost:9001) (MinIO Console).
2. Sign in with the credentials from `docker-compose.yaml`:
   - **User:** `admin`
   - **Password:** `supersecretpassword`
3. In the MinIO console, open **Buckets** in the left menu, then **Create Bucket**.
4. Name the bucket **`unified-bharat`** and create it.

The S3 API for tools that need an endpoint is available at `http://localhost:9000` (same credentials unless you change them in the compose file).

### Step 3: JupyterLab and the PySpark / Iceberg workflow

Connect Spark to MinIO using your notebooks (for example Apache Iceberg as the table format).

1. Read the Spark/Jupyter container logs to get the JupyterLab URL and token:

   ```bash
   docker logs spark_compute
   ```

2. In the log output, find the line with a URL like `http://127.0.0.1:8888/lab?token=...` (or `http://localhost:8888/?token=...`).
3. Open that URL in your browser and run your medallion pipeline notebooks from the mounted workspace (`./notebooks` on the host maps to `/home/jovyan/work` in the container).

Host folders mounted into the notebook container:

- `./notebooks` → `/home/jovyan/work`
- `./data` → `/home/jovyan/data`

### Stop the stack

```bash
docker-compose down
```

To remove named volumes as well (this deletes MinIO data stored in the `minio_data` volume):

```bash
docker-compose down -v
```

## Prerequisites

- Python 3.10+ (recommended)
- NDAP API key
- NDAP base URL (QA or production)

## Setup

From the repository root:

```bash
pip install -r requirements.txt
```

Create your local env file:

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

macOS/Linux:

```bash
cp .env.example .env
```

Then edit `.env`:

- `NDAP_BASE_URL` - e.g. `https://loadqa.ndapapi.com/v1/openapi`
- `NDAP_API_KEY` - exact value used in the `API_Key` query parameter from NDAP

## Configure sources

Edit `test/ndap/sources.yaml`.

`defaults` controls parameter names used in requests. NDAP commonly uses:

- `api_key_param: API_Key`
- `indicators_param: ind`
- `page_param: pageno`

Each item in `sources` supports:

- `id` - unique source name
- `enabled` - whether to fetch this source
- `indicators` - indicator id(s), such as `I6008_6`
- `output` - output JSON filename (written to `test/ndap_data/`)
- `extra_params` - additional NDAP query parameters (for example `dim`)

## Usage

Run all enabled sources:

```bash
python test/ndap/fetch_all.py
```

Run one source:

```bash
python test/ndap/fetch_all.py --only csr_district
```

Run with a custom config file (must be inside this project folder):

```bash
python test/ndap/fetch_all.py --config test/ndap/sources.yaml
```

Limit pages for a quick test:

```bash
python test/ndap/fetch_all.py --only csr_district --max-pages 2
```

Disable CSV export:

```bash
python test/ndap/fetch_all.py --no-csv
```

## Output format

For each source (example `csr_district`), the fetcher writes:

- `test/ndap_data/csr_district.json`
- `test/ndap_data/csr_district.csv`

JSON includes metadata plus:

- `pages` - raw page payloads from NDAP
- `records` - merged table rows across pages

CSV contains flattened row data:

- nested objects (like `I6008_6`) become dotted columns
- example: `I6008_6.sum`, `I6008_6.avg`
- UTF-8 with BOM is used so Excel opens Unicode correctly on Windows

## Notes on "empty" or null values

- Many NDAP rows can be validly zero (`sum: 0`, `avg: 0`) for a given
  district-year-category slice.
- `null` in fields like `stddev` or some weight columns is common in NDAP.
- This does not mean fetch failed; check `record_count` and the CLI summary
  line for non-zero `sum` counts.

## Troubleshooting

- `ERROR: Set NDAP_API_KEY ...`  
  Add `NDAP_API_KEY` in `.env`.
- `ERROR: Set NDAP_BASE_URL ...`  
  Add `NDAP_BASE_URL` in `.env`.
- Output looks sparse in QA  
  Try production NDAP base URL if available for your account.
- A source is skipped  
  Ensure `enabled: true` and non-placeholder `indicators` in `sources.yaml`.

## Security

- Never commit real API keys.
- Keep secrets only in `.env`.
- `.env.example` should remain empty for sensitive values.

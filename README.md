# Databricks Asset Bundle — ETL Template

A ready-to-use template for deploying PySpark ETL jobs on Databricks using [Databricks Asset Bundles (DAB)](https://docs.databricks.com/en/dev-tools/bundles/index.html) with GitHub Actions CI/CD.

The included example reads NYC taxi data, computes trip durations, and aggregates fares by zipcode — but the structure is designed to be swapped out for your own ETL logic.

> Based on the tutorial [Building CI Pipeline with Databricks Asset Bundle and GitLab](https://hackernoon.com/building-ci-pipeline-with-databricks-asset-bundle-and-gitlab) by **bnu** on HackerNoon. Adapted to use GitHub Actions instead of GitLab CI.

---

## Project Structure

```
├── databricks.yml                  # Bundle configuration (entry point for DAB)
├── resources/
│   └── dev_jobs.yml                # Job definitions (etl_job + test_job)
├── my_package/
│   ├── common.py                   # Base Task class (SparkSession, read/write helpers)
│   └── tasks/
│       └── sample_etl_job.py       # ETL logic (NYC taxi data)
├── notebooks/
│   ├── run_unit_test.py            # Runs pytest on Databricks
│   └── explorative_analysis.py     # EDA notebook (runs after ETL)
├── tests/
│   ├── conftest.py                 # Spark session fixture
│   └── test_sample.py              # Unit tests for the ETL job
├── setup.py                        # Python wheel packaging + entry points
└── .github/workflows/ci.yml        # GitHub Actions pipeline
```

---

## Prerequisites

1. **Databricks workspace** with an existing all-purpose cluster
2. **Databricks CLI** (v0.205+):
   ```bash
   brew tap databricks/tap
   brew install databricks
   ```
   Verify: `databricks -v`
3. **Python 3.11+**

---

## Setup

### 1. Clone the repo

```bash
git clone <your-repo-url>
cd assest_bundle-tutorial-main
```

### 2. Configure Databricks authentication

Create (or edit) `~/.databrickscfg`:

```ini
[asset-bundle-tutorial]
host = https://<your-workspace>.cloud.databricks.com
token = <your-personal-access-token>
```

To generate a token: **Databricks UI** → **User Settings** → **Developer** → **Access tokens** → **Generate new token**.

### 3. Set your cluster ID

Open `resources/dev_jobs.yml` and replace the placeholder:

```yaml
variables:
  my_cluster_id:
    description: The ID of an existing cluster.
    default: <ADD YOUR CLUSTER ID>   # ← replace this
```

You can find your cluster ID in **Databricks UI** → **Compute** → click your cluster → the ID is in the URL.

---

## Local Development

Install the package with dev dependencies:

```bash
pip install -e ".[local]"
```

Run tests locally (requires a local Spark installation):

```bash
pytest tests/ -v
```

> **Note:** The tests run the full ETL against the NYC taxi dataset on Databricks. Running locally requires access to `dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled`.

---

## Bundle Commands

All commands use the Databricks CLI.

**Validate** — check your config for errors:
```bash
databricks bundle validate
```

**Deploy** — build the wheel, upload files, and create jobs in your workspace:
```bash
databricks bundle deploy -t dev
```

**Run** — execute a job:
```bash
databricks bundle run -t dev etl_job
databricks bundle run -t dev test_job
```

**Destroy** — remove all deployed resources:
```bash
databricks bundle destroy --auto-approve
```

> In `dev` mode, all resources are prefixed with `[dev <your_username>]` so they don't clash with other developers.

---

## GitHub Actions CI/CD

The pipeline (`.github/workflows/ci.yml`) runs on every push to `main`:

| Stage    | What it does                                         |
|----------|------------------------------------------------------|
| **test** | Deploys the bundle, runs `test_job` on Databricks    |
| **deploy** | Deploys the bundle, runs `etl_job` on Databricks   |

The deploy stage only runs if tests pass.

### Required secrets

Add these in your GitHub repo → **Settings** → **Secrets and variables** → **Actions**:

| Secret              | Value                                           |
|---------------------|-------------------------------------------------|
| `DATABRICKS_HOST`   | `https://<your-workspace>.cloud.databricks.com` |
| `DATABRICKS_TOKEN`  | Your personal access token                      |

---

## How to Customize for Your Own ETL

1. **Add your task** — create a new file in `my_package/tasks/`, subclass `Task` from `common.py`, and implement `launch()`
2. **Add an entry point** — register it in `setup.py` under `console_scripts`
3. **Add a job definition** — create a new YAML in `resources/` or add to `dev_jobs.yml`
4. **Add tests** — add test functions in `tests/`
5. **Deploy** — `databricks bundle deploy -t dev && databricks bundle run -t dev <your_job>`

---

## What Each File Does

| File | Purpose |
|------|---------|
| `databricks.yml` | Defines the bundle name, workspace profile, includes, and targets |
| `resources/dev_jobs.yml` | Declares two jobs: `etl_job` (wheel task + EDA notebook) and `test_job` (pytest notebook) |
| `my_package/common.py` | Abstract `Task` class with SparkSession, logger, and table read/write helpers |
| `my_package/tasks/sample_etl_job.py` | Reads NYC taxi data, adds trip duration, aggregates by zipcode |
| `notebooks/explorative_analysis.py` | EDA notebook that visualizes aggregated results (runs after ETL) |
| `notebooks/run_unit_test.py` | Databricks notebook that runs pytest against the deployed code |
| `setup.py` | Packages `my_package` as a wheel with the `etl_job` entry point |
| `tests/test_sample.py` | Validates ETL outputs: duration column, zipcodes, aggregations |

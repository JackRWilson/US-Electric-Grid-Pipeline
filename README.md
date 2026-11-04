# U.S. Electricity Demand & Generation Analysis

A data-driven analysis on U.S. electricity demand and generation, and how weather, geography, population, and income relate to energy use.

The project pulls hourly electricity data from the EIA API and weather data from Open-Meteo, combines them with static datasets (population, income, balancing authority information, etc.), loads everything
into a PostgreSQL database, and visualizes the results in an interactive dashboard. The pipeline is orchestrated with Apache Airflow and runs hourly.

![Dashboard screenshot](docs/dashboard.png)

> ### This is an overview. View the [written report](docs/report.md) for analysis.

---

## Questions & Takeaways

| Question | Takeaways |
| ----- | ----- |
| 1. How does demand and generation vary by area? | Total demand and generation scale with BA size. Normalized demand is consistent across BAs, and most demand more than they generate. |
| 2. How does weather affect demand, generation, and renewables? | Demand follows temperature and has an estimated balance point of 50°F. Wind and cloud conditions show slight correlation with renewable generation. |
| 3. Can weather help predict electricity use? | Demand varies with temperature, making forecasting useful. However, demand does not significantly vary with weather condition. |
| 4. Does population affect electricity use? | There is little relationship between BA population estimates and demand per person. |
| 5. Do higher income areas use more electricity? | There is little relationship between estimated income and demand per person. |

---

## Tools
| Category | Tools |
| ----- | ----- |
| Language | Python, SQL |
| APIs | EIA API, Open-Mateo API |
| Database | PostgreSQL, DBeaver |
| Orchestration | Apache Airflow | 
| Dashboard | Dash, Plotly |

---

## Project Structure

```text
.
├─ airflow/
│  ├─ requirements.txt
│  └─ ...
├─ app/
│  ├─ assets/
│  ├─ main.py # Run app
│  ├─ requirements.txt # Install for app
│  └─ ...
├─ dags/
│  └─ electricity_etl_dag.py
├─ data/ # Static data for manual setup
├─ db/
│  ├─ electricity_erd.png
│  ├─ electricity_schema.sql # Run database schema
│  └─ ...
├─ docs/
│  ├─ plan.md
│  ├─ report.md # Report
│  └─ ...
├─ scripts/
│  └─ download_static.py
├─ src/
│  └─ electricity/
│     ├─ main.py # Run pipeline
│     └─ ...
├─ .env.example
├─ .gitignore
├─ compose.yaml
├─ README.md # You are HERE
├─ start.py # Start/stop the container stack
└─ requirements.txt # Install for pipeline
```

---

## Set Up

### Prerequisites

- Python 3.11 or newer for the launcher
- Free [EIA API key](https://www.eia.gov/opendata/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose (recommended)
- SQL client such as [DBeaver](https://dbeaver.io/download/) (optional)

### Quick start

1. Start Docker Desktop and open a terminal in the repository folder.

2. Run the launcher:

   ```bash
   python start.py
   ```

3. If prompted, add your EIA API key to `.env` and run `python start.py` again.

4. If not opened automatically, open:
   - Dashboard: <http://localhost:8050/>
   - Airflow: <http://localhost:8080/> (credentials are printed by the launcher)

5. Press Enter in the launcher window to stop the containers.

See [configuration details](#configuration-and-initial-startup) for `.env` options and [managing services](#managing-services) for restarting, troubleshooting, and reset instructions. For running without Docker, see [manual setup](#manual-setup).

### Container setup

#### Configuration details

On the first run, the launcher creates `.env` from `.env.example`. Set `EIA_API_KEY` there and leave other settings as they are.

If EIA is temporarily unavailable, retry later or bypass the check:

```bash
python start.py --skip-eia-check
```

**Never commit `.env`. Default database credentials are for local use only.**

*(Optional) `EIA_INITIAL_START` and `WEATHER_INITIAL_START` set the initial backfill dates in `YYYY-MM-DD` format, otherwise the ETL uses the most recent 90 days.*

On first startup Docker builds the images and downloads static files into a persistent volume. Airflow runs the ETL hourly. The initial load can take about **1.5 hours**.

#### Managing services

If running the launcher without an interactive terminal, stop the stack from the
repository root with:

```bash
docker compose stop
```

To start the stack directly without the Python launcher, run:

```bash
docker compose up -d --build
```

Use `docker compose start` and `docker compose stop` for later starts and stops.

Check service status and logs with:

```bash
docker compose ps
docker compose logs -f airflow app data-init
```

To retrieve the Airflow credentials again:

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

To browse the database in DBeaver, connect to `localhost` on `DB_PORT` (default `5433`)
using `DB_USER`, `DB_PASSWORD`, and `DB_DATABASE` from `.env`.

The database, downloaded files, Airflow state, and ETL cache persist in Docker volumes.
Running `python start.py` again resumes existing containers.

To rebuild images after code or dependency changes:

```bash
python start.py --build
```

To completely reset the container installation, including the database and downloaded
static files:

```bash
docker compose down --volumes --remove-orphans
```

**This permanently deletes the Docker volumes.** It does not delete `.env` or files in
the local `data/` folder. Omit `--volumes` if you want to keep pipeline state.

### Manual setup

Manual setup runs Python on the host. It requires a running PostgreSQL database,
the six static data files listed below in `data/`, and a configured `.env`. It does
not use the Docker downloader or automatically schedule hourly ETL runs.

1. Download these files into the `data/` folder:

   | File | Source | Notes |
   | ----- | :-----: | :-----: |
   | `2___Plant_Y2025.xlsx` | [EIA-860 archive](https://www.eia.gov/electricity/data/eia860/xls/eia8602025.zip) | Extract this from the zip |
   | `CAINC1__ALL_AREAS_1969_2024.csv` | [BEA archive](https://apps.bea.gov/regional/zip/CAINC1.zip) | Extract this from the zip |
   | `sub-est2025.csv` | [Census population estimates](https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/cities/totals/sub-est2025.csv) | Download as is |
   | `2020_Gaz_counties_national.txt` | [Census Gazetteer archive](https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_counties_national.zip) | Extract this from the zip |
   | `Balancing_Authority_2025.xlsx` | [EIA-861 archive](https://www.eia.gov/electricity/data/eia861/zip/f8612025.zip) | Extract this from the zip |
   | `out_eia861__yearly_balancing_authority_service_territory.parquet` | [PUDL parquet file](https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/out_eia861__yearly_balancing_authority_service_territory.parquet) | Download as is |

2. Create and activate a virtual environment, then install dependencies:

   ```bash
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   pip install -r app/requirements.txt
   ```

3. Copy `.env.example` to `.env`, enter your EIA API key, and set the `DB_*` values to match your local PostgreSQL database. Update `DB_ENGINE_URL` if your connection differs.

4. Run `db/electricity_schema.sql` using DBeaver or another SQL client. Then run the ETL:

   ```bash
   python src/electricity/main.py
   ```

5. In another terminal with the virtual environment activated, run the dashboard:

   ```bash
   python app/main.py
   ```

   Open <http://localhost:8050/>. To refresh the data, run the ETL again as needed.

---

## Data Sources
| Source | Used For |
| ----- | ----- |
| [EIA Open Data API](https://www.eia.gov/opendata/) | Hourly demand and generation by fuel type |
| [Open-Meteo Historical API](https://open-meteo.com/en/docs/historical-forecast-api) | Weather data |
| [EIA-860 Plant Data (2025)](https://www.eia.gov/electricity/data/eia860/) | Power plant locations |
| [Census Population Estimates](https://www.census.gov/data/tables/time-series/demo/popest/2020s-total-cities-and-towns.html) | Population by county |
| [Census Gazetteer Files](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.2020.html#list-tab-264479560) | County geography |
| [PUDL BA Service Territories](https://data.catalyst.coop/preview/pudl/out_eia861__yearly_balancing_authority_service_territory) | Balancing Authority territories |
| [EIA-861](https://www.eia.gov/electricity/data/eia861/) | Additional Balancing Authority territory data |
| [BEA Regional Income (CAINC1)](https://apps.bea.gov/regional/zip/CAINC1.zip) | Income by county |

---

## Troubleshooting
 
> **__Airflow login fails:__** run `docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated` to view the generated credentials again.

> **Cannot connect to the database:** confirm `DB_PORT` is `5433` and that the database container is running.

> **Missing file errors on first run:** check the `data-init` container logs with `docker compose logs data-init`. It downloads the required static files into a persistent volume.

> **Dashboard still shows its waiting page:** check `docker compose ps` and `docker compose logs -f airflow db`. The waiting page disappears after the initial ETL run has written the `static` watermark.

> **EIA key check fails:** verify `EIA_API_KEY` in `.env`. If EIA is unreachable rather than rejecting the key, use `python start.py --skip-eia-check` to continue.

> **Docker services do not start:** make sure Docker Desktop is running, then inspect `docker compose logs`.

> **Airflow pipeline fails:** this happens if the computer is shut off. Restart the containers with `docker compose restart`.

Football Data ETL Project

This project builds an end-to-end mini data engineering pipeline for football data using the free Football-Data.org API.

It fetches team, match, and player data for the Premier League and loads it into a local DuckDB database. The goal is to practise ETL design, data modelling, and automation using Python, dbt, Airflow, and Power BI.

Project Structure

football_project/

config/
config.yaml # API key and DB connection details

data/
football.duckdb # Local DuckDB database

etl/
init.py
full_etl.py # Main ETL pipeline

logs/
football_etl_log_*.log # Timestamped run logs

tests/
test_etl.py # Basic pytest coverage for key functions

requirements.txt
README.md

How It Works

Fetch data

Pulls team, match, and player data from the API.

Respects API rate limits (10 calls per minute).

Includes retry and error-handling logic.

Store data

Saves results in DuckDB (default).

Optionally configurable for Snowflake later.

Logging

Logs all steps to both console and file (logs/football_etl_log_YYYYMMDDHHMMSS.log).

Configuration

Settings live in config/config.yaml.

Example content for config/config.yaml:

api:
token: "YOUR_API_KEY_HERE"

database:
type: "duckdb"
duckdb_path: "./data/football.duckdb"

etl:
requests_per_min: 10
retries_per_team: 3
sleep_between_retries: 2

Running the ETL

In your virtual environment, from the project root:

python etl/full_etl.py

Logs will appear in logs/.

To explore data manually from Python:

import duckdb
con = duckdb.connect('data/football.duckdb')
con.sql("SELECT name, tla FROM teams LIMIT 5;").df()

Running Tests

pytest -v

Tests currently validate:

Config loading

Database connection creation and table writes

Next Steps

Add incremental loading logic

Set up dbt for transformations

Orchestrate ETL with Airflow or Task Scheduler

Connect Power BI directly to DuckDB or Snowflake

License

MIT License — use freely for learning and personal projects.

Author

Christopher Almond
GitHub: cs01cma
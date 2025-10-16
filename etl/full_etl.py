"""
Full ETL script for Premier League Football Data
------------------------------------------------
Fetches teams, matches, and players from Football-Data.org API,
loads them into DuckDB or Snowflake depending on configuration.

Features:
- Logging to both console and timestamped file
- Rate limiting and retry logic for API calls
- Config-driven database connections
- Docstrings and inline comments for clarity
"""

import os
import time
import logging
from datetime import datetime
import requests
import pandas as pd
import yaml

# -------------------------
# Logging setup
# -------------------------
os.makedirs("../logs", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
log_filename = f"../logs/football_etl_log_{timestamp}.log"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# File handler (new file each run)
file_handler = logging.FileHandler(log_filename, mode="w")
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(file_formatter)
logger.addHandler(console_handler)

logger.propagate = False  # prevent double output in Jupyter
logging.info(f"Logging initialized. Logs will go to {log_filename} and console.")

# -------------------------
# Load configuration
# -------------------------
def load_config(config_path="../config/config.yaml"):
    """Load YAML configuration file."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config

# -------------------------
# Database connection
# -------------------------
def get_db_connection(config):
    """Create a database connection based on config."""
    db_type = config['database']['type'].lower()

    if db_type == 'duckdb':
        import duckdb
        os.makedirs(os.path.dirname(config['database']['duckdb_path']), exist_ok=True)
        con = duckdb.connect(database=config['database']['duckdb_path'])
        logging.info(f"Connected to DuckDB at {config['database']['duckdb_path']}")
        return con

    elif db_type == 'snowflake':
        import snowflake.connector
        con = snowflake.connector.connect(
            user=config['database']['snowflake']['user'],
            password=config['database']['snowflake']['password'],
            account=config['database']['snowflake']['account'],
            database=config['database']['snowflake']['database'],
            warehouse=config['database']['snowflake']['warehouse']
        )
        logging.info("Connected to Snowflake")
        return con

    else:
        raise ValueError(f"Unsupported database type: {db_type}")

# -------------------------
# API Fetch Functions
# -------------------------
def fetch_json(url, headers, retries=3, sleep_between=2):
    """Fetch JSON data from a URL with retry logic."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.json()
            else:
                logging.warning(f"Attempt {attempt+1} failed: {resp.status_code} for {url}")
        except requests.RequestException as e:
            logging.error(f"Request exception: {e}")
        time.sleep(sleep_between)
    logging.error(f"Failed to fetch: {url}")
    return None

def fetch_teams(headers, con, db_type):
    """Fetch Premier League teams and store in database."""
    url = "https://api.football-data.org/v4/competitions/PL/teams"
    data = fetch_json(url, headers)
    if data and "teams" in data:
        df_teams = pd.json_normalize(data['teams'])
        if db_type == "duckdb":
            con.execute("CREATE OR REPLACE TABLE teams AS SELECT * FROM df_teams")
        elif db_type == "snowflake":
            con.cursor().execute("CREATE OR REPLACE TABLE teams AS SELECT * FROM df_teams")
        logging.info("Teams table updated in DB.")
        return df_teams
    else:
        logging.error("Teams data not available.")
        return pd.DataFrame()

def fetch_matches(headers, con, db_type, season=2025):
    """Fetch Premier League matches for a season and store in database."""
    url = f"https://api.football-data.org/v4/competitions/PL/matches?season={season}"
    data = fetch_json(url, headers)
    if data and "matches" in data:
        df_matches = pd.json_normalize(data['matches'])
        if db_type == "duckdb":
            con.execute("CREATE OR REPLACE TABLE matches AS SELECT * FROM df_matches")
        elif db_type == "snowflake":
            con.cursor().execute("CREATE OR REPLACE TABLE matches AS SELECT * FROM df_matches")
        logging.info("Matches table updated in DB.")
        return df_matches
    else:
        logging.error("Matches data not available.")
        return pd.DataFrame()

def fetch_team_players(team_id, headers, retries=3, sleep_between=2):
    """Fetch players for a single team."""
    for attempt in range(retries):
        try:
            url = f"https://api.football-data.org/v4/teams/{team_id}"
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                squad = resp.json().get('squad', [])
                for player in squad:
                    player['team_id'] = team_id
                return squad
            else:
                logging.warning(f"Attempt {attempt+1} failed for team {team_id}: {resp.status_code}")
        except requests.RequestException as e:
            logging.error(f"Request exception for team {team_id}: {e}")
        time.sleep(sleep_between)
    logging.warning(f"All attempts failed for team {team_id}")
    return []

def fetch_all_players(df_teams, headers, con, db_type, requests_per_min=10, retries=3, sleep_between=2):
    """Fetch players for all teams with rate limiting and store in database."""
    all_players = []
    failed_teams = []
    sleep_between_calls = 60 / requests_per_min

    for i, team_id in enumerate(df_teams['id'], start=1):
        players = fetch_team_players(team_id, headers, retries, sleep_between)
        if players:
            all_players.extend(players)
        else:
            failed_teams.append(team_id)
        logging.info(f"Processed {i}/{len(df_teams)} teams")
        time.sleep(sleep_between_calls)

    df_players = pd.json_normalize(all_players).drop_duplicates(subset=['id'])

    if db_type == "duckdb":
        con.execute("CREATE OR REPLACE TABLE players AS SELECT * FROM df_players")
    elif db_type == "snowflake":
        con.cursor().execute("CREATE OR REPLACE TABLE players AS SELECT * FROM df_players")

    logging.info("Players table updated in DB.")

    if failed_teams:
        missing_team_names = df_teams[df_teams['id'].isin(failed_teams)][['id','name']]
        logging.warning("Teams missing players this run:")
        logging.warning("\n%s", missing_team_names)

    return df_players

# -------------------------
# Main ETL orchestrator
# -------------------------
def main(config_dict=None):
    """Main ETL function."""
    if config_dict is None:
        config = load_config()
    else:
        config = config_dict

    API_TOKEN = config['api']['token']
    HEADERS = {"X-Auth-Token": API_TOKEN}
    REQUESTS_PER_MIN = config['etl']['requests_per_min']
    RETRIES_PER_TEAM = config['etl']['retries_per_team']
    SLEEP_BETWEEN_RETRIES = config['etl']['sleep_between_retries']

    con = None
    try:
        con = get_db_connection(config)
        db_type = config['database']['type'].lower()

        df_teams = fetch_teams(HEADERS, con, db_type)
        if df_teams.empty:
            logging.error("No teams fetched. Aborting ETL.")
            return

        df_matches = fetch_matches(HEADERS, con, db_type)
        df_players = fetch_all_players(
            df_teams, HEADERS, con, db_type,
            requests_per_min=REQUESTS_PER_MIN,
            retries=RETRIES_PER_TEAM,
            sleep_between=SLEEP_BETWEEN_RETRIES
        )

        logging.info("ETL run complete.")

    except Exception as e:
        logging.error(f"ETL failed: {e}", exc_info=True)

    finally:
        if con is not None:
            try:
                con.close()
                logging.info("Database connection closed.")
            except Exception as e:
                logging.warning(f"Failed to close connection: {e}")

# -------------------------
# CLI run
# -------------------------
if __name__ == "__main__":
    main()

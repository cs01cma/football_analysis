# tests/test_etl.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import tempfile
import yaml
import duckdb
import pathlib
from etl.full_etl import load_config, get_db_connection

def test_load_config_from_tempfile():
    # create a temporary config file and ensure load_config reads it
    tmpdir = tempfile.TemporaryDirectory()
    cfg_path = os.path.join(tmpdir.name, "test_config.yaml")
    config_data = {
        "api": {"token": "TEST_TOKEN"},
        "etl": {"requests_per_min": 10, "retries_per_team": 1, "sleep_between_retries": 0},
        "database": {"type": "duckdb", "duckdb_path": os.path.join(tmpdir.name, "tmp.duckdb")}
    }
    with open(cfg_path, "w") as f:
        yaml.safe_dump(config_data, f)

    loaded = load_config(config_path=cfg_path)
    assert loaded["api"]["token"] == "TEST_TOKEN"
    assert loaded["database"]["type"] == "duckdb"

    tmpdir.cleanup()

def test_get_db_connection_creates_duckdb(tmp_path):
    # Build a minimal config dict that points to a tmp duckdb path
    db_path = tmp_path / "test_duckdb.db"
    config = {
        "database": {
            "type": "duckdb",
            "duckdb_path": str(db_path)
        }
    }

    con = get_db_connection(config)
    # run a simple statement to ensure connection works
    con.execute("CREATE TABLE IF NOT EXISTS t_test (id INTEGER, name TEXT)")
    con.execute("INSERT INTO t_test VALUES (1, 'a'), (2, 'b')")
    res = con.execute("SELECT COUNT(*) FROM t_test").fetchone()[0]
    assert res == 2

    con.close()
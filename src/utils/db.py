from datetime import datetime
import enum
import sqlite3
import pandas as pd
from src.utils.log_util import log

DATABASE_NAME = "smartt.db"


class OnConflictTypes(enum.Enum):
    UPDATE = "UPDATE"
    IGNORE = "NOTHING"


def convert_timestamp(val: pd.Timestamp):
    return val.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")


sqlite3.register_adapter(pd.Timestamp, convert_timestamp)


def connect_to_db() -> sqlite3.Connection:
    with sqlite3.connect(DATABASE_NAME, autocommit=False) as con:
        return con


def migrate():
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS energy_source(
        source TEXT PRIMARY KEY,
        is_offshore NOT NULL DEFAULT FALSE,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at)
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS energy_production(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        start TEXT,
        end TEXT,
        source_name TEXT,
        production_amount REAL,
        production_unit TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT, 
        FOREIGN KEY(source_name) REFERENCES energy_source(source)
        )
        """
    )
    cur.close()
    conn.commit()


def insert_into_db(data: pd.DataFrame, table: str, on_conflict: OnConflictTypes):
    if len(data) == 0:
        log.error("Empty dataframe cannot be inserted")
        return

    data = data.to_dict("records")
    col_names = ", ".join(data[0].keys())
    col_binding_names = ",".join([":" + field_name for field_name in data[0].keys()])

    curr = conn.cursor()
    try:
        if on_conflict == OnConflictTypes.IGNORE:
            curr.executemany(
                f"INSERT INTO {table.lower()}({col_names}) VALUES ({col_binding_names}) ON CONFLICT DO {on_conflict.value}",
                data,
            )
        elif on_conflict == OnConflictTypes.UPDATE:
            updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql_update_statement = ", ".join(
                [f"{col_name}=excluded.{col_name}" for col_name in data[0].keys()]
            )
            curr.executemany(
                f"INSERT INTO {table.lower()}({col_names}) VALUES ({col_binding_names}) ON CONFLICT DO {on_conflict.value} SET {sql_update_statement}, updated_at = '{updated_at}' ",
                data,
            )
        else:
            log.error(f"{OnConflictTypes.name} does not exists!")
    except Exception as exc:
        log.error(exc)
    curr.close()
    conn.commit()


try:
    conn = connect_to_db()
    log.info(f"Connection to the {DATABASE_NAME} has been successfully made.")
except Exception as exc:
    log.critical(exc)

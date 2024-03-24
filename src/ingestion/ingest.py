import math
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
from pathlib import Path

from src.ingestion.const import *
from src.utils.log_util import log


class Ingestion:
    def __init__(self):
        self.production_data: pd.DataFrame = pd.DataFrame()

    def ingest(
        self,
        from_date: datetime,
        to_date: datetime,
        data_format: str = "CSV",
        region="DE",
        resolution="quarterhour",
    ):
        from_date_str = from_date.strftime("%Y-%m-%d")
        to_date_str = to_date.strftime("%Y-%m-%d")
        ingested_file_name = f"data/energy_production_{from_date_str}_{to_date_str}.csv"
        ingestion_folder = Path("src").absolute().joinpath(ingested_file_name)
        from_date_timestamp: int = math.floor(from_date.timestamp() * 10e2)
        to_date_timestamp: int = math.floor(to_date.timestamp() * 10e2)
        res = requests.post(
            BASE_URL,
            json={
                "request_form": [
                    {
                        "format": data_format,
                        "moduleIds": [
                            1001224,
                            1004066,
                            1004067,
                            1004068,
                            1001223,
                            1004069,
                            1004071,
                            1004070,
                            1001226,
                            1001228,
                            1001227,
                            1001225,
                        ],
                        "region": region,
                        "timestamp_from": from_date_timestamp,
                        "timestamp_to": to_date_timestamp,
                        "type": "discrete",
                        "language": "en",
                        "resolution": resolution,
                    }
                ]
            },
        )
        self.production_data = pd.read_csv(StringIO(res.text), sep=";", na_values=["-"])
        log.info(f"{ingested_file_name} has been ingested!")
        self.production_data.to_csv(ingestion_folder, index=False)

    def ingest_all(self):
        current_date = datetime.now()
        for year in range(2022, current_date.year + 1):
            for month in range(1, 13):
                if month > current_date.month and year >= current_date.year:
                    break
                from_date = datetime(year, month, day=1)
                to_date = from_date + timedelta(days=31)
                time.sleep(5)
                self.ingest(from_date, to_date)

import requests
from typing import Optional
from src.utils.log_util import log
from src.utils.db import insert_into_db
from src.utils.db import OnConflictTypes
from datetime import datetime
import pandas as pd


class Pipeline:
    def __init__(self, raw_data: pd.DataFrame):
        self._raw_data = raw_data.copy()

    def extract_transform_source_information(self) -> pd.DataFrame:
        col_names = self._raw_data
        transformed_col_names = []
        for col_name in col_names:
            is_offshore = False
            if "Original resolutions" in col_name:
                col_name = col_name.replace("Original resolutions", "")
                if "offshore" in col_name:
                    is_offshore = True
                transformed_col_names.append(
                    {"source": col_name, "is_offshore": is_offshore}
                )
        return pd.DataFrame(transformed_col_names)

    def run(self):
        sources = self.extract_transform_source_information()
        insert_into_db(sources, "energy_source", on_conflict=OnConflictTypes.IGNORE)
        pass

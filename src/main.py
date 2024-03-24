from argparse import ArgumentParser
from os import getenv
from pathlib import Path

import pandas as pd
from src.pipeline import Pipeline
from src.utils import log, migrate
from src.ingestion.ingest import Ingestion
from src.digestion.digest import Digestion

API_KEY = getenv("API_KEY")

if __name__ == "__main__":
    parser = ArgumentParser()
    log.info("Migrating the database!")
    migrate()

    # ingestion = Ingestion()
    # ingestion.ingest_all()

    digest = Digestion(fp=Path("src/data").absolute())
    for fl in digest.files:
        raw_data = pd.read_csv(fl, sep=",")
        pl = Pipeline(raw_data)
        pl.run()

    pass

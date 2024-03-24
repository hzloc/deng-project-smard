import logging
import sys
from typing import Optional


def logger(name: str, level: Optional = 0):
    log = logging.getLogger(name)
    log.setLevel(level=level)

    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s - %(filename)s - %(levelname)s - %(message)s")
    handler.setFormatter(fmt)

    if not log.hasHandlers():
        log.addHandler(handler)

    return log


log = logger("app", 20)

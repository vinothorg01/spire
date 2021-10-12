from spire.tasks import run_scoring
import yaml
import datetime
from pathlib import Path
from spire.config import config

if __name__ == "__main__":
    run_date = datetime.datetime.now() - datetime.timedelta(days=1)
    run_scoring(run_date)

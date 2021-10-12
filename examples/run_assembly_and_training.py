import yaml
from spire.tasks import run_assembly_and_training
from spire.config import config
import datetime
from pathlib import Path


def get_config():
    file_dir = Path(__file__).parent
    config_path = (
        file_dir
        / f"../include/spire-astronomer/include/cfg/{config.DEPLOYMENT_ENV}/orchestration.yml"
    )
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


if __name__ == "__main__":

    run_date = datetime.datetime.now()
    config = get_config()
    training_config = config["training"]
    assembly_config = config["assembly"]
    run_assembly_and_training(run_date, assembly_config, training_config)

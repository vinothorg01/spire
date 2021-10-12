import os
import yaml
import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_load_initial_conftests(args, early_config, parser):
    try:
        path = os.path.join(os.getcwd(), "tests/plugins", "env_vars.yml")
        with open(path, "r") as v:
            env_vars = yaml.safe_load(v)
        for key, val in env_vars.items():
            # Allow DB_HOSTNAME to be override for test
            # Prevent anything else from being overriden to prevent accidental
            # non-test database from being wipe.
            if key == "DB_HOSTNAME" and key in os.environ:
                continue
            os.environ[key] = val
    except Exception:
        return None

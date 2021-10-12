import os
import hvac

from spire.utils import Logger
from spire.version import __version__ as version


class DefaultConfig(object):
    DEPLOYMENT_ENV = os.environ.get("DEPLOYMENT_ENV", "development")
    SPIRE_ENVIRON = os.environ.get("SPIRE_ENVIRON", "cn-spire-dev")
    SPIRE_DATA = os.environ.get("SPIRE_DATA", "cn-spire-data-dev")

    VAULT_TOKEN: str = None
    VAULT_ADDRESS: str = None
    VAULT_READ_PATH: str = None
    SPIRE_VERSION: str = version

    DB_HOSTNAME = os.environ.get("DB_HOSTNAME", "localhost")
    DB_USER = os.environ.get("DB_USER", "postgres")
    DB_PASSWORD = os.environ.get("DB_PASSWORD", None)
    DB_NAME = os.environ.get("DB_NAME", "spire_db")

    POLLING_PERIOD_INTERVAL = int(os.environ.get("SPIRE__POLLING_PERIOD_INTERVAL", 30))
    THREADPOOL_GROUP_SIZE = int(os.environ.get("SPIRE__THREADPOOL_GROUP_SIZE", 20))
    WF_THRESHOLD = int(os.environ.get("SPIRE__WF_THRESHOLD", 100))
    WF_THRESHOLD_MIN = int(os.environ.get("SPIRE__WF_THRESHOLD_MIN", 1))
    API_CALL_TRY_COUNT = int(os.environ.get("SPIRE__API_CALL_TRY_COUNT", 3))
    INTER_API_CALL_SLEEP_TIME = int(
        os.environ.get("SPIRE__INTER_API_CALL_SLEEP_TIME", 20)
    )
    ERROR_PERCENT_THRESHOLD = int(os.environ.get("SPIRE__ERROR_PERCENT_THRESHOLD", 0.3))

    # Github Containery Registry
    GHCR_USER = os.environ.get("GHCR_USER")
    GHCR_PASSWORD = os.environ.get("GHCR_PASSWORD")

    DATABRICKS_HOST = None
    DATABRICKS_TOKEN = None

    # GIT BRANCH
    GIT_ID = os.environ.get("GIT_ID", "main")


class DefaultRemoteConfig(DefaultConfig, Logger):
    def __init__(self):
        self.VAULT_TOKEN = os.environ.get("VAULT_TOKEN", self.VAULT_TOKEN)
        self.VAULT_ADDRESS = os.environ.get("VAULT_ADDRESS", self.VAULT_ADDRESS)
        self.VAULT_READ_PATH = os.environ.get("VAULT_READ_PATH", self.VAULT_READ_PATH)

        try:
            client = hvac.Client(url=self.VAULT_ADDRESS, token=self.VAULT_TOKEN)
            client.renew_token()
            vault_data = client.read(self.VAULT_READ_PATH)["data"]
        except Exception as e:
            self.logger.error(self.__str__(), exc_info=True)
            raise e

        self.DB_HOSTNAME = os.environ.get("DB_HOSTNAME", "localhost")
        self.DB_USER = vault_data["user"]
        self.DB_NAME = vault_data["db"]
        self.DB_PASSWORD = vault_data["pwd"]

        self.DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
        self.DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

        self.GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_API_REFRESH_TOKEN")
        self.GOOGLE_TOKEN_URI = os.environ.get("GOOGLE_API_TOKEN_URI")
        self.GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_API_CLIENT_ID")
        self.GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_API_CLIENT_SECRET")

        self.SLACK_TOKEN = os.environ.get("SLACK_TOKEN")

        self.logger.info(self.__str__())

    def __str__(self):
        if self.VAULT_TOKEN:
            trunc_vault_token = ("X" * (len(self.VAULT_TOKEN) - 4)) + self.VAULT_TOKEN[
                -4:
            ]
        else:
            trunc_vault_token = None
        return (
            f"<{type(self)}(VAULT_TOKEN='{trunc_vault_token}', "
            f" VAULT_ADDRESS='{self.VAULT_ADDRESS}', VAULT_READ_PATH="
            f"'{self.VAULT_READ_PATH}', DB_HOSTNAME={self.DB_HOSTNAME},"
            f" DB_USER={self.DB_USER}, DB_NAME={self.DB_NAME}, "
            f"SPIRE_VERSION={self.SPIRE_VERSION})>"
        )

    def __repr__(self):
        return self.__str__()

    def update(self):
        self.__init__()


class DevelopmentConfig(DefaultRemoteConfig):
    def __init__(self):
        self.DEPLOYMENT_ENV = "development"
        self.SPIRE_ENVIRON = "cn-spire-dev"
        self.SPIRE_DATA = "cn-spire-data-dev"
        self.VAULT_READ_PATH = "secret/data-innovation/spire/development/db"
        super().__init__()


class StagingConfig(DefaultRemoteConfig):
    def __init__(self):
        self.DEPLOYMENT_ENV = "staging"
        self.SPIRE_ENVIRON = "cn-spire-staging"
        self.SPIRE_DATA = "cn-spire-data-staging"
        self.VAULT_READ_PATH = "secret/data-innovation/spire/staging/db"
        super().__init__()


class ProductionConfig(DefaultRemoteConfig):
    def __init__(self):
        self.DEPLOYMENT_ENV = "production"
        self.SPIRE_ENVIRON = "cn-spire"
        self.SPIRE_DATA = "cn-spire-data"
        self.VAULT_READ_PATH = "secret/data-innovation/spire/production/db"
        super().__init__()


class CliConfig(DefaultRemoteConfig):
    VAULT_READ_PATH = "secret/data-innovation/spire/development/db"


ENV = os.environ.get("DEPLOYMENT_ENV", "default")


if os.environ.get("CLI_INSTANCE"):
    config = CliConfig()
elif ENV == "production":
    config = ProductionConfig()
elif ENV == "staging":
    config = StagingConfig()
elif ENV == "development":
    config = DevelopmentConfig()
else:
    config = DefaultConfig()

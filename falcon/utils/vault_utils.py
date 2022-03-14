import os
import hvac


class VaultAccess:
    def __init__(self, mode):
        self.mode = mode
        self.VAULT_ADDR = os.getenv("VAULT_ADDR")

    @property
    def VAULT_TOKEN(self):

        vault_token = os.getenv("VAULT_TOKEN_PROD") if self.mode == "prod" else os.getenv("VAULT_TOKEN_STG") \
            if self.mode=="stg" else os.getenv("VAULT_TOKEN_DEV")
        return vault_token

    def get_settings(self, settings_type="socialflow"):
        """

        Args:
            settings_type: Can be "aws", "rds", "socialflow"

        Returns:

        """
        settings_location = f"secret/data-innovation/falcon-e2/{'development' if self.mode=='dev' else 'staging' if self.mode=='stg' else 'production'}/{settings_type}"

        client = hvac.Client(url=self.VAULT_ADDR, token=self.VAULT_TOKEN)
        client.renew_token()
        settings = client.read(settings_location).get("data")
        return settings

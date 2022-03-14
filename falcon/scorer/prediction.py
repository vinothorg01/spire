import mlflow
import pandas as pd
from ..logger.logger import FalconLogger

class Scorer:

    def __init__(self, brand_name, social_platform_name, mode='dev', model_version=None, common_model=True):
        self.model_name = f"falcon_{brand_name}_{social_platform_name}_logistic_prod"
        self.model_version = model_version  ## in future change the version with mode
        self.scope = 'mlflow-scope'
        self.key = 'model-registry'
        self.registry_uri = 'databricks://' + self.scope + ':' + self.key if self.scope and self.key else None
        self.logger = FalconLogger()
        if mode == 'stg':
            self.model_version = "Staging"
        elif mode == "prod":
            self.model_version = "Production"
        else:
            if model_version is None:
                model_version = "Staging"
                self.logger.info("Model version is None, Hence Reading STAGING version of the model")
            self.model_version = model_version

        if common_model:
            mlflow.set_registry_uri(self.registry_uri)

    def predict(self, prediction_traffic_df, features):
        """

        :param common_model: set to true to access common model across all envs
        :param prediction_traffic_df:
        :param features:
        :return:  dataframe with sorted(desc) order by model score
        """


        model_prod = mlflow.sklearn.load_model(
            model_uri=f"models:/{self.model_name}/{self.model_version}"
        )

        X_predict = prediction_traffic_df.loc[:, features]

        prediction_traffic_df_probs = pd.Series(
            model_prod.predict_proba(X_predict)[:, 1],
            index=prediction_traffic_df.index,
            name="model_score",
        )

        prediction_traffic_df = pd.concat(
            [prediction_traffic_df, prediction_traffic_df_probs], axis=1
        )

        idx = prediction_traffic_df.groupby("cid")["model_score"].idxmax()
        outcomes = prediction_traffic_df.loc[idx, :].sort_values(
            by="model_score", ascending=False
        )

        return outcomes


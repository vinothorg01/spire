# Databricks notebook source
# DBTITLE 1,Import: Base Functions
# MAGIC %run ../../functions/sample_functions

# COMMAND ----------

# DBTITLE 1,Get Config Info
ENV = dbutils.widgets.get('env')
CONFIG = json.loads( dbutils.notebook.run( '../../../config/sample_config', 60, {'env': ENV} ) )

CURRENT_DATE = str(datetime.date( datetime.now() )) if ENV == 'dev' else dbutils.widgets.get( 'run_date' )
VAULT_STRING = None if ENV == 'dev' else dbutils.widgets.get( 'vault_string' )

vault_dict = get_vault_dict( ENV,
                             VAULT_STRING,
                             CONFIG['S3_KEY_PATH'] )

# Databricks notebook source
brand_name = dbutils.widgets.get("brand_name")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

from multiprocessing.pool import ThreadPool
from datetime import date, timedelta, datetime

# COMMAND ----------

def check_success_outputs(outputs_list):
  total_output_sum = 0
  for val in outputs_list:
    if val == "Success":
      total_output_sum += 1
  
  if total_output_sum == len(outputs_list):
    return True
  else:
    return False

# COMMAND ----------

def run_multithreaded_social_platforms():

  threads = 40
  notebook_prediction = "m_4_Falcon_Predictions_Brands"
  
  social_platforms = ['facebook_page', 'twitter']
  social_partner = 'socialflow'
  data_for = 'predict'
  
  pool = ThreadPool(threads)

  final_outputs = pool.map(lambda arg: dbutils.notebook.run(notebook_prediction, 1800, arguments={'brand_name': brand_name, 'mode': mode, 'social_platform_name': arg, 'social_partner_name': social_partner, 'data_for': data_for}), social_platforms)
  if check_success_outputs(final_outputs):
    print("Final Success !!!")
  else:
    raise Exception("Prediction Notebook Failed !!!")

# COMMAND ----------

if __name__ == '__main__':
  run_multithreaded_social_platforms()

# COMMAND ----------

dbutils.notebook.exit("Success")

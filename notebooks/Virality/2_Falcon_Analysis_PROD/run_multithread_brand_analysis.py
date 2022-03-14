# Databricks notebook source
# dbutils.widgets.dropdown("brand_name", "self", ['self', 
#                                                 'teen-vogue', 
#                                                 'conde-nast-traveler', 
#                                                 'allure', 
#                                                 'architectural-digest', 
#                                                 'vanity-fair', 
#                                                 'epicurious', 
#                                                 'vogue', 
#                                                 'pitchfork', 
#                                                 'gq', 
#                                                 'wired', 
#                                                 'new-yorker', 
#                                                 'glamour', 
#                                                 'bon-appetit',
#                                                 'vogue-uk',
#                                                 'gq-uk',
#                                                 'them'
#                                                ])
# dbutils.widgets.dropdown("mode", "dev", ['dev', 'prod'])

# COMMAND ----------

brand_name = dbutils.widgets.get("brand_name")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

from multiprocessing.pool import ThreadPool
from datetime import date, timedelta, datetime

# COMMAND ----------

def run_multithreaded_social_platforms():

  threads = 40
  notebook_prediction = "4_Falcon_LifetimeMetrics_Socialflow_Final"
  
  social_platforms = ['facebook_page', 'twitter']
  social_partner = 'socialflow'

  
  pool = ThreadPool(threads)

  final_outputs = pool.map(lambda arg: dbutils.notebook.run(notebook_prediction, 1800, arguments={'brand_name': brand_name, 'mode': mode, 'social_platform_name': arg, 'social_partner_name': social_partner}), social_platforms)

# COMMAND ----------

if __name__ == '__main__':
  run_multithreaded_social_platforms()


# COMMAND ----------

# dbutils.notebook.exit("Success")

# COMMAND ----------



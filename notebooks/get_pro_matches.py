# Databricks notebook source
import requests
import pandas as pd
import datetime

url = "https://api.opendota.com/api/proMatches"

resp = requests.get(url)
df = pd.DataFrame(resp.json())

# COMMAND ----------

df.head()

# COMMAND ----------

class ingestor_api():

    def __init__(self, url, to_stop):
        self.url = url
        self.to_stop = datetime.datetime.strptime(to_stop, "%Y-%m-%d")
   
    def get_data(self, **kwargs):
        resp = requests.get(self.url, params=kwargs)
        return resp
    
    def save_data(self, data):
        path = './data/'
        df = spark.createDataFrame(data)
        df.coalesce(1).write.format("parquet").mode("append").save(path)

    def get_and_save(self, **kwargs):
        resp = self.get_resp(**kwargs)
        data = resp.json()
        self.save_data(data)

# COMMAND ----------

ing = ingestor_api(url, '2023-07-01')
ing
# resp = ing.get_data()
# ing.save_data(resp.json())

# COMMAND ----------

df["match_id"].min()

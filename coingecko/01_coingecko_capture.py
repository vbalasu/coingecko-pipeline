# Databricks notebook source
# MAGIC %pip install lxml requests

# COMMAND ----------

# MAGIC %md
# MAGIC ### Capture from coingecko.com into json on dbfs
# MAGIC 
# MAGIC The first step is to use Pandas to read html from coingecko.com. Note that you need to specify a user agent in the request to avoid a forbidden message
# MAGIC 
# MAGIC Add an `as_of` column to capture the time when the data was pulled

# COMMAND ----------

# https://stackoverflow.com/questions/43590153/http-error-403-forbidden-when-reading-html
url = 'https://www.coingecko.com/'
import pandas as pd, requests
header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}
r = requests.get(url, headers=header)
dfs = pd.read_html(r.text)
df = dfs[0].copy()
import datetime
df['as_of'] = datetime.datetime.utcnow().isoformat()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the file into json records format
# MAGIC 
# MAGIC The filename is based on the current UTC timestamp

# COMMAND ----------

import datetime
with open(f'/dbfs/coingecko/{datetime.datetime.utcnow().isoformat()}.json', 'w') as f:
  f.write(df.to_json(orient='records'))

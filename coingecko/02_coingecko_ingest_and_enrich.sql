-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Ingest into bronze table (append only)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE coins_bronze TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
) AS
SELECT *, input_file_name() file_name FROM cloud_files("/coingecko/", "json");

-- COMMAND ----------

--DROP TABLE vijay_stock_portfolio.coins_bronze;
--SELECT * FROM vijay_stock_portfolio.coins_bronze;

-- COMMAND ----------

-- CREATE OR REPLACE FUNCTION clean_up (numeric_string STRING)
-- RETURNS DOUBLE
-- RETURN double(replace(replace(replace(numeric_string, '$', ''), ',', ''), '%', ''));
--DESCRIBE FUNCTION EXTENDED clean_up;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Enrich into silver table
-- MAGIC 
-- MAGIC Clean up data types, keep only latest version

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE coins_silver AS
SELECT `#` rank, Coin coin, clean_up(Price) price, clean_up(`Mkt Cap`) mkt_cap, clean_up(`24h Volume`) volume_24h, 
clean_up(`1h`) change_1h, clean_up(`24h`) change_24h, clean_up(`7d`) change_7d, as_of
FROM live.coins_bronze
WHERE as_of = (SELECT MAX(as_of) FROM live.coins_bronze);

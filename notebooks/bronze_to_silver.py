# Databricks notebook source
display(dbutils.fs.ls('/mnt/dados/Bronze'))

# COMMAND ----------

path = '/mnt/dados/Bronze/dataset_imoveis/'
df = spark.read.format('delta').load(path)
display(df)

# COMMAND ----------

display(df.select('anuncio.*','anuncio.endereco.*'))

# COMMAND ----------

dados_detalhados = df.select('anuncio.*','anuncio.endereco.*')

# COMMAND ----------

df_silver = dados_detalhados.drop('caracteristicas','endereco')

# COMMAND ----------

path = '/mnt/dados/Silver/dataset_imoveis'
df_silver.write.format('delta').mode('overwrite').save(path)

# COMMAND ----------



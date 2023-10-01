# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Conferindo se os dados foram montados e se temos acesso a pasta inbound

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dados/Inbound"))

# COMMAND ----------

path = '/mnt/dados/Inbound'
json_inbound = spark.read.json(path)

display(json_inbound)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Removendo Colunas

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####SQL

# COMMAND ----------

json_inbound.createOrReplaceTempView('json_1')

# COMMAND ----------

formata_coluna = spark.sql('SELECT * EXCEPT(IMAGENS,USUARIO) FROM json_1')
formata_coluna.createOrReplaceTempView('json_2')

# COMMAND ----------

spark.sql('''SELECT * FROM json_2 ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####PySpark

# COMMAND ----------

json_colunas = json_inbound.drop('imagens', 'usuario')
display(json_colunas)

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

df_bronze = json_colunas.withColumn('id',col('anuncio.id'))
display(df_bronze)

# COMMAND ----------

path = '/mnt/dados/Bronze/dataset_imoveis'
df_bronze.write.format('delta').mode('overwrite').save(path)

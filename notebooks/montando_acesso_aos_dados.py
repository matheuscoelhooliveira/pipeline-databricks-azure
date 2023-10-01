# Databricks notebook source
dbutils.fs.mkdirs("/mnt/dados")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dados"))

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "2180e5a8-3c1d-4dde-bdd2-78c063d18aa3",
          "fs.azure.account.oauth2.client.secret":"h~_8Q~uk2Ov3XrJsIwXedWeEIcGPALojbZVKpbcO",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9e3ce0bc-bdde-4333-917a-9e91b7f7a46a/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://imoveis@datalakealura2023.dfs.core.windows.net/",
  mount_point = "/mnt/dados",
  extra_configs = configs)

# COMMAND ----------



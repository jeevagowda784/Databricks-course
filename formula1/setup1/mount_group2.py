# Databricks notebook source
storage_account_name = "formulagroup2"
client_id            = dbutils.secrets.get(scope="group_demoscope",key="demogroupsecret1client")
tenant_id            = dbutils.secrets.get(scope="group_demoscope",key="dempoappsecret2tenant")
client_secret        = dbutils.secrets.get(scope="group_demoscope",key="demoappsecret3cli")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

def mountadls(container_name):
    
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mountadls("raw")

# COMMAND ----------

mountadls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formulagroup2/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formulagroup2/processed")

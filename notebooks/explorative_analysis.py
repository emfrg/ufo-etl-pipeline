# Databricks notebook source
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# COMMAND ----------

# MAGIC %md
# MAGIC ## explore the number trips  

# COMMAND ----------

df_nyctaxi_zip_agg = spark.table("nyc_taxi_zip_agg_vals")

# COMMAND ----------

display(df_nyctaxi_zip_agg)

# COMMAND ----------

pd_df_nyctaxi_zip_agg = df_nyctaxi_zip_agg.toPandas()
display(pd_df_nyctaxi_zip_agg)

# COMMAND ----------

# "Melt" the dataset to "long-form" or "tidy" representation
melted_pd_df = pd_df_nyctaxi_zip_agg.melt(id_vars="pickup_zip", var_name="measurement")
display(melted_pd_df)

# COMMAND ----------

f, ax = plt.subplots(figsize=(7, 5))
sns.despine(f)

sns.boxenplot(
    data=melted_pd_df,
    x="measurement", y="value",
)

ax.set_yscale("log")

# COMMAND ----------

# Initialize the figure
f, ax = plt.subplots()
sns.despine(bottom=True, left=True)
sns.stripplot(
    data=melted_pd_df, y="measurement", x="value",
    dodge=True, alpha=.25, zorder=1,)

ax.set_xscale("log")

# COMMAND ----------



# Databricks notebook source
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## UFO Sightings — Exploratory Data Analysis

# COMMAND ----------

df_agg = spark.table("ufo_sightings_agg").toPandas()
df_cleaned = spark.table("ufo_sightings_cleaned").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Top 20 States by Sighting Count

# COMMAND ----------

top_states = (
    df_agg.groupby("state")["sighting_count"]
    .sum()
    .sort_values(ascending=False)
    .head(20)
)

f, ax = plt.subplots(figsize=(12, 6))
top_states.plot(kind="bar", ax=ax)
ax.set_title("Top 20 States by UFO Sighting Count")
ax.set_xlabel("State")
ax.set_ylabel("Total Sightings")
plt.xticks(rotation=45)
plt.tight_layout()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Sightings by Decade and Shape (Heatmap)

# COMMAND ----------

pivot = df_cleaned.pivot_table(
    index="decade",
    columns="shape",
    values="year",
    aggfunc="count",
    fill_value=0,
)
# Keep only the top 10 most common shapes to avoid clutter
top_shapes = df_cleaned["shape"].value_counts().head(10).index
pivot = pivot[top_shapes]

f, ax = plt.subplots(figsize=(14, 8))
sns.heatmap(pivot, cmap="YlOrRd", linewidths=0.5, ax=ax, fmt="d", annot=True)
ax.set_title("UFO Sightings: Decade x Shape")
ax.set_xlabel("Shape")
ax.set_ylabel("Decade")
plt.tight_layout()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Average Duration by Shape (Strip Plot, Log Scale)

# COMMAND ----------

top_shape_agg = df_agg[df_agg["shape"].isin(top_shapes)]

f, ax = plt.subplots(figsize=(12, 6))
sns.stripplot(data=top_shape_agg, x="shape", y="avg_duration", alpha=0.5, ax=ax)
ax.set_yscale("log")
ax.set_title("Average Sighting Duration by Shape (Log Scale)")
ax.set_xlabel("Shape")
ax.set_ylabel("Avg Duration (seconds)")
plt.xticks(rotation=45)
plt.tight_layout()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Sightings Per Year Over Time

# COMMAND ----------

yearly = (
    df_cleaned.groupby("year")["state"]
    .count()
    .reset_index()
    .rename(columns={"state": "sighting_count"})
)
yearly = yearly[yearly["year"] > 1940]  # Filter out sparse early data

f, ax = plt.subplots(figsize=(14, 5))
ax.plot(yearly["year"], yearly["sighting_count"], linewidth=1.5)
ax.set_title("UFO Sightings Per Year")
ax.set_xlabel("Year")
ax.set_ylabel("Number of Sightings")
ax.grid(True, alpha=0.3)
plt.tight_layout()

# COMMAND ----------



# Databricks notebook source
import os
import sys

# COMMAND ----------

# Add project root to sys.path so my_package is importable.
notebook_path = '/Workspace/' + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path.insert(0, os.path.dirname(notebook_path))

# COMMAND ----------

from my_package.tasks.ufo_etl_job import entrypoint
entrypoint()

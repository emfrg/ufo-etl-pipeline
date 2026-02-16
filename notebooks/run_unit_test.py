# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

#
import pytest
import os
import sys
#from tests.conftest import DBUtilsFixture

notebook_path =  '/Workspace/' + os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
print(notebook_path)
%cd $notebook_path
# Get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))

# Prepare to run pytest from the repo.
print(os.getcwd())

# Add project root to sys.path so my_package is importable.
sys.path.insert(0, os.path.dirname(notebook_path))

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main(["../tests", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."

# COMMAND ----------



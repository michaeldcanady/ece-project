import os
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)
# Airflow puts its DAG folder on sys.path, which is how the DAGs import their shared `_common`.
sys.path.insert(0, os.path.join(ROOT, "dags"))

import requests
import os
import logging
import pandas as pd
from sqlalchemy import text, create_engine


url = os.getenv('DATABRICKS_DB_URL')
engine = create_engine(url, connect_args={"base_parameters": {"query_timeout": 30}})

try:
    engine.connect()
    print('Connection Successful')
except Exception as e:
    print(f'Error when connecting to the database {e}')

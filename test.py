import requests
import os
import logging
import pandas as pd
from sqlalchemy import text, create_engine


def sync_fact_job_postings(df):
    if df.empty:
        print("No data to sync for FactJobPosting.")
        return
    
    target_table = "fact_job_postings"
    staging_table = f"staging_{target_table}"
    
    # 1. Safety: Drop staging if it exists from a previous failed run
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
    
    # 2. Push to Staging with Batching (Fast)
    df.to_sql(
        staging_table, 
        con=engine, 
        if_exists='replace', 
        index=False, 
        method='multi', 
        chunksize=1000
    )
    
    # 3. Native MERGE (Handling Updates & Inserts)
    merge_sql = text(f"""
        MERGE INTO {target_table} AS target
        USING {staging_table} AS source
        ON target.job_id = source.job_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.is_expired = source.is_expired,
                target.last_seen_id = source.last_seen_id
        WHEN NOT MATCHED THEN
            INSERT (job_id, job_title, is_expired, job_link, last_seen_id, location_id, label_id, emp_id, created_on_id)
            VALUES (source.job_id, source.job_title, source.is_expired, source.job_link, source.last_seen_id, 
                    source.location_id, source.label_id, source.emp_id, source.created_on_id)
    """)
    
    with engine.begin() as conn:
        conn.execute(merge_sql)
        conn.execute(text(f"DROP TABLE {staging_table}"))
    
    print(f"Successfully synced {target_table}")


def sync_fact_skill_fast(df):
    if df.empty:
        return
    
    staging_table = "staging_fact_skill"
    target_table = "fact_skill"
    
    # 1. Clear staging for safety
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
    
    # 2. Bulk upload to staging
    df.to_sql(staging_table, con=engine, if_exists='replace', index=False, method='multi')

    # 3. Fast "Not Exists" Insert
    # This query only inserts rows where the combination of skill_id AND job_id doesn't exist yet
    insert_sql = text(f"""
        INSERT INTO {target_table} (skill_id, job_id)
        SELECT s.skill_id, s.job_id
        FROM {staging_table} s
        WHERE NOT EXISTS (
            SELECT 1 FROM {target_table} t
            WHERE t.skill_id = s.skill_id 
              AND t.job_id = s.job_id
        )
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql)
        conn.execute(text(f"DROP TABLE {staging_table}"))
        
    print(f"Successfully synced {target_table}")


def databricks_hybrid_upsert(df, target_table, unique_key, columns_to_update):
    """
    df: The pandas DataFrame (e.g., location_dim_df)
    target_table: The table name in Databricks
    unique_key: The column to match on (e.g., 'location_name')
    columns_to_update: List of columns to update if match found (excluding the ID)
    """
    if df.empty:
        return
    
    staging_table = f"staging_{target_table}"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
    
    # 1. Push to Staging
    df.to_sql(staging_table, con=engine, if_exists='replace', index=False, method='multi')
    
    # 2. Build the MERGE query
    # Logic: Match on unique_key. If exists, update metadata. If not, insert.
    update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in columns_to_update])
    insert_cols = ", ".join(columns_to_update + [unique_key])
    insert_vals = ", ".join([f"source.{col}" for col in columns_to_update + [unique_key]])

    merge_sql = text(f"""
        MERGE INTO {target_table} AS target
        USING {staging_table} AS source
        ON target.{unique_key} = source.{unique_key}
        WHEN MATCHED THEN
            UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    with engine.begin() as conn:
        conn.execute(merge_sql)
        conn.execute(text(f"DROP TABLE {staging_table}"))
    
    print(f"Successfully synced {target_table}")


url = os.getenv('DATABRICKS_DB_URL')
engine = create_engine(url, connect_args={"base_parameters": {"query_timeout": 30}})

try:
    engine.connect()
    print('Connection Successful')
except Exception as e:
    print(f'Error when connecting to the database {e}')



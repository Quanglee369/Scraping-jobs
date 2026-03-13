import requests
import os
import logging
import pandas as pd
from datetime import datetime
from clean_data_functions import safe_literal_eval
from sqlalchemy import text, create_engine


def sync_fact_job_postings(df):
    if df.empty:
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
    
    target_table = "fact_skill"
    staging_table = f"staging_{target_table}"
    
    
    # 1. Clear staging for safety
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
    
    # 2. Bulk upload to staging
    try:
        df.to_sql(staging_table, con=engine, if_exists='replace', index=False, method='multi')
        print('[sync_fact_skill_fast] Success updating staging table')
    except Exception as e:
        print(f"[sync_fact_skill_fast] Error when updating staging table, error: {e}")

    # 3. Fast "Not Exists" Insert
    # This query only inserts rows where the combination of skill_id AND job_id doesn't exist yet
    try:
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
            
        print(f"[sync_fact_skill_fast] Successfully synced {target_table}")
    except Exception as e:
        print(f"[sync_fact_skill_fast] Error in updating main database, error: {e}")


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
    try:
        df.to_sql(staging_table, con=engine, if_exists='replace', index=False, method='multi')
        print("[databricks_hybrid_upsert] Success updating staging table")
    except Exception as e:
        print(f"[databricks_hybrid_upsert] Failed to update staging table, error: {e}")

    # 2. Build the MERGE query
    # Logic: Match on unique_key. If exists, update metadata. If not, insert.
    update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in columns_to_update])
    insert_cols = ", ".join(columns_to_update + [unique_key])
    insert_vals = ", ".join([f"source.{col}" for col in columns_to_update + [unique_key]])

    try:
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
        
        print(f"[databricks_hybrid_upsert] Successfully synced {target_table}")
    except Exception as e:
        print("[databricks_hybrid_upsert] Fail to update main databse, error: {e}")


url = os.getenv('DATABRICKS_DB_URL')
engine = create_engine(url, connect_args={"base_parameters": {"query_timeout": 30}})

try:
    engine.connect()
    print('Connection Successful')
except Exception as e:
    print(f'Error when connecting to the database {e}')


df = pd.read_csv('/workspaces/Scraping-jobs/df_master.csv')
df['date_view'] = pd.to_datetime(df['date_view'], utc=True, format = 'ISO8601').dt.tz_convert('Asia/Ho_Chi_Minh').dt.date

date_dim_dict = pd.read_sql_query("Select * from date_dim", engine)
emp_dim_dict = pd.read_sql_query("Select * from emp_dim", engine)
location_dim_dict = pd.read_sql_query("Select * from location_dim", engine)
label_dim_dict = pd.read_sql_query("Select * from label_dim", engine)
skill_dim_dict = pd.read_sql_query("Select * from skill_dim", engine)

df['skills'] = df['skills'].apply(safe_literal_eval)
df['last_seen'] = datetime.today().date()
df['is_expired'] = False

df5 = (df
    .merge(date_dim_dict, how='inner', left_on='date_view', right_on='actual_date')
    .drop(columns=['actual_date'])
    .rename(columns= {'date_id': 'created_on_id'})
    .merge(emp_dim_dict, how='inner', left_on='emp_name', right_on='emp_raw')
    .merge(location_dim_dict, how='inner', on='location_name')
    .merge(label_dim_dict, how='inner', left_on='label', right_on='label_name')
    .merge(date_dim_dict, left_on='last_seen', right_on='actual_date')
    .rename(columns={'date_id': 'last_seen_id'})
    # Drop all the 'raw' columns used for joining in one go
    .drop(columns=[
        'date_view', 'actual_date', 'last_seen','emp_name', 'emp_raw', 
        'emp_cleaned', 'location_name', 'label', 'label_name'
    ])
)
df_skill = df5[['job_id', 'skills']].explode('skills').reset_index(drop=True)
df_skill.rename(columns = {'skills': 'skill_raw'}, inplace=True)
df_skill['skill_raw'] = df_skill['skill_raw'].str.strip()
df_skill_final = df_skill.merge(skill_dim_dict, on='skill_raw', how='inner').drop(columns= ['skill_raw', 'skill_cleaned'])

df5 = df5.drop(columns = 'skills')

print(df5.head(15))
print(df5.columns)
print(df_skill_final.head(15))
print(df_skill_final.columns)

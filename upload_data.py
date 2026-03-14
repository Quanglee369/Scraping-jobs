import os
import logging
import pandas as pd
from datetime import datetime
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy import text, create_engine
from sklearn.metrics.pairwise import cosine_similarity
from clean_data_functions import prep_data_dim, fast_remove_accents, safe_literal_eval, databricks_hybrid_upsert, sync_fact_job_postings, sync_fact_skill_fast

# Configure logging
logging.basicConfig(
    level = logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_debug.log", mode='a'),
        logging.StreamHandler()
    ],
    force = True
)

# Check if master data exist, if not create dummy dataframe to avoid error in the next step of data cleaning
if not os.path.exists('df_master.csv'):
    df_master = pd.DataFrame()
    logging.error('[check_master_data] df_master.csv does not exist, create dummy dataframe')
else:
    df_master = pd.read_csv('df_master.csv')
    print('[check_master_data] df_master exist, begin to check for duplicate data')

# Combination of jobtitle and company name (emp_name)
compare_text = (df_master['job_title'].str.lower().str.replace(' ', '') + " " +
df_master['emp_name'].str.lower().str.replace(' ', ''))


# Check for cosine similarity to filter out if one company post the same job in different websites
vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(3,3))
tfidf_matrix = vectorizer.fit_transform(compare_text)
cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

# Filter out if the combination of job title and company name has similarity higher than 0.7
key = set()
drop_index = set()
for i in range(len(cosine_sim)):
  for u in np.where(cosine_sim[i] > 0.75)[0]:
    if i != u and u not in key: # Only add if the duplicate value not in the base case
      key.add(int(i))
      drop_index.add(int(u))

# Drop duplicate jobs
df_master.drop(index = list(drop_index), axis = 0, inplace= True)

# Convert string into list for the skill column
df_master['skills'] = df_master['skills'].apply(safe_literal_eval)

# Convert date into ISO8601 format
df_master['date_view']= pd.to_datetime(df_master['date_view'], utc=True, format = 'ISO8601').dt.tz_convert('Asia/Ho_Chi_Minh').dt.date

# Standardize location name
df_master['location_name'] = df_master['location_name'].apply(fast_remove_accents).str.split(" - ").apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)

# Create additional column to check if last_seen not equal today then the job is expired -> set is_expire to True
df_master['last_seen'] = datetime.today().date()
df_master['is_expired'] = False

df_master.rename(columns = {'label': 'label_name', 'emp_name': 'emp_raw'}, inplace=True)

# Explode and seperate skills data for storage efficiency
df_skills = df_master[['job_id', 'skills']].explode('skills').reset_index(drop=True)
df_skills['jobskill_id'] = df_skills['skills'].astype(str) + df_skills['job_id'].astype(str)
df_skills.rename(columns = {'skills': 'skill_raw'}, inplace=True)
df_skills['skill_raw'] = df_skills['skill_raw'].str.strip()

df_master = df_master.drop(columns='skills')

# Initiate sessions and connect to supabase database
url = os.getenv('DATABRICKS_DB_URL')
engine = create_engine(url, connect_args={"base_parameters": {"query_timeout": 30}})

# Prepare data to input into databrick
list_of_dim_cols = ['location_name', 'date_view', 'last_seen', 'label_name', 'emp_raw']
results = prep_data_dim(data=df_master, collist=list_of_dim_cols)
location_name = results[0]
date_view = results[1]
last_seen = results[2]
label = results[3]
emp_name = results[4]

list_of_dim_cols_skills = ['skill_raw']
result_skills = prep_data_dim(data=df_skills, collist=list_of_dim_cols_skills)
skills = result_skills[0]


# Test the connection
try:
    engine.connect()
    print('[testing_database_connection] Connection Successful')
except Exception as e:
    print(f'[testing_database_connection] Error when connecting to the database {e}')


# Create list of tasks to update dimensional data into the tables
databricks_hybrid_upsert(df = location_name, target_table='location_dim', unique_key='location_name', columns_to_update=['location_name'], engine= engine)
databricks_hybrid_upsert(df = date_view, target_table='date_dim', unique_key='actual_date', columns_to_update=['actual_date'], engine= engine)
databricks_hybrid_upsert(df = last_seen, target_table='date_dim', unique_key='actual_date', columns_to_update=['actual_date'], engine= engine)
databricks_hybrid_upsert(df = label, target_table='label_dim', unique_key='label_name', columns_to_update=['label_name'], engine= engine)
databricks_hybrid_upsert(df = emp_name, target_table='emp_dim', unique_key='emp_raw', columns_to_update=['emp_raw'], engine= engine)
databricks_hybrid_upsert(df = skills, target_table='skill_dim', unique_key='skill_raw', columns_to_update=['skill_raw'], engine= engine)


# Prepare data for mapping and get ids from the newly updated dimensional tables
date_dim_dict = pd.read_sql_query("Select * from date_dim", engine)
emp_dim_dict = pd.read_sql_query("Select * from emp_dim", engine)
location_dim_dict = pd.read_sql_query("Select * from location_dim", engine)
label_dim_dict = pd.read_sql_query("Select * from label_dim", engine)
skill_dim_dict = pd.read_sql_query("Select * from skill_dim", engine)

# Map the data for both fact_job_posting and fact_skill table
update_fact_job_postings = (df_master
    .merge(date_dim_dict, how='left', left_on='date_view', right_on='actual_date')
    .drop(columns=['actual_date'])
    .rename(columns= {'date_id': 'created_on_id'})
    .merge(emp_dim_dict, how='left', left_on='emp_raw', right_on='emp_raw')
    .merge(location_dim_dict, how='left', on='location_name')
    .merge(label_dim_dict, how='left', left_on='label_name', right_on='label_name')
    .merge(date_dim_dict, left_on='last_seen', right_on='actual_date')
    .rename(columns={'date_id': 'last_seen_id'})
    # Drop all the 'raw' columns used for joining in one go
    .drop(columns=[
        'date_view', 'actual_date', 'last_seen','emp_raw',
        'emp_cleaned', 'location_name', 'label_name'
    ])
)

update_fact_skill = (df_skills
                .merge(skill_dim_dict, on = 'skill_raw', how='left')
                .drop(columns=['skill_cleaned', 'skill_raw'])
                .reset_index(drop=True)
)

# Check column types - look for 'float64' where it should be 'int'
logging.warning(f"DATAFRAME TYPES:\n{update_fact_job_postings.dtypes}")

# Check for NaNs in label_id (can be due to exceed AI limit) that turn Integers into Floats, fill nan with 0
if update_fact_job_postings['label_id'].isnull().any():
  nan_count = update_fact_job_postings['label_id'].isnull().sum()
  logging.error(f"⚠️ Column label_id contains {nan_count} NaN values! Proceed to fill na")

update_fact_job_postings['label_id'] = update_fact_job_postings['label_id'].fillna(0).astype(int)

# Begin to update fact_job_postings table
sync_fact_job_postings(update_fact_job_postings, engine= engine)

# Begin to update fact_skill table
sync_fact_skill_fast(update_fact_skill, engine= engine)
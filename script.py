import pandas as pd
import os
import asyncio
from datetime import datetime
import time
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import aiohttp 
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine, Column, Integer, Text, Boolean, ForeignKey, Date
from sqlalchemy.orm import declarative_base, sessionmaker
import logging
from retrieve_data_functions import fetch_job_headers, run_itviec_scraper, chunking, get_all_jobs, process_all_at_once
from clean_data_functions import filter_relevant, remove_duplicate, extract_skills_from_jd, fill_label, prep_data_dim, fast_remove_accents



# Configure logging
logging.basicConfig(
    level = logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_debug.log"),
        logging.StreamHandler()
    ],
    force = True
)



# Extract data from careerviet and vietnamworks using asyncio for speed
async def main():
    platforms = {'careerviet': ['data analyst', 'data engineer', 'data scientist', 'ai', 'sql'],
             'vietnamworks': ['data analyst', 'data engineer', 'data scientist', 'ai', 'sql']}
    keywords = ['data analyst', 'data engineer', 'data scientist', 'ai', 'sql']
    data_itviec = []

    for i in keywords:
        result = await run_itviec_scraper(i)
        data_itviec.append(result)

    async with aiohttp.ClientSession() as session:
        task_cv_vn = [fetch_job_headers(session=session, keyword= u, platform= i) for i, keyword_list in platforms.items() for u in keyword_list ]
        data_cv_vn = await(asyncio.gather(*task_cv_vn))
        return data_cv_vn, data_itviec
if __name__ == "__main__":
    data_cv_vn, data_itviec = asyncio.run(main())

# Adding data from extracted result to form a list of dicts
all_data_itviec = []
for i in data_itviec:
  all_data_itviec.extend(i)

careerviet = []
vietnamworks = []
for i in data_cv_vn:
  if i.get('careerviet'):
    careerviet.extend(i.get('careerviet'))
  else:
    vietnamworks.extend(i.get('vietnamworks'))

# Filter out irrelevant data
filtered_data_cv = filter_relevant(careerviet, 'careerviet')
filtered_data_itviec = filter_relevant(all_data_itviec, 'itviec')
filtered_data_vn = filter_relevant(vietnamworks, 'vietnamworks')

# Remove duplicate data, prepare input data for AI relabling
filtered_vn, input_ai_vn = remove_duplicate(filtered_data_vn, platform= 'vietnamworks')
filtered_itviec, input_ai_itviec = remove_duplicate(filtered_data_itviec, platform= 'itviec')
filtered_cv, input_ai_cv = remove_duplicate(filtered_data_cv, platform= 'careerviet')

# Extract skills from JD specifically for careerviet
async def execute_extract_skills():
    alljdcv = extract_skills_from_jd(await get_all_jobs(filtered_cv))
    return alljdcv

alljdcv = asyncio.run(execute_extract_skills())
for i, u in zip(filtered_cv, alljdcv):
  if i.get('job_id') == u.get('job_id'):
    i['skills'] = u.get('skills')

# Sending data for AI to relabel jobs
labeled_cv = []
for i in chunking(input_ai_cv):
  labeled_cv.extend(process_all_at_once(i))
  time.sleep(5) # Adding a slight delay after each loop prevent overloading the API
time.sleep(20) # Delay after each batch to reset limit

labeled_vn = []
for i in chunking(input_ai_vn):
  labeled_vn.extend(process_all_at_once(i))
  time.sleep(5)
time.sleep(20)

labeled_itviec = []
for i in chunking(input_ai_itviec):
  labeled_itviec.extend(process_all_at_once(i))
  time.sleep(5)
time.sleep(20)

# Merge all AI output into a dictionary
labeled_cv_cleaned = {key:value for i in labeled_cv for key, value in i.items()}
labeled_vn_cleaned = {key:value for i in labeled_vn for key, value in i.items()}
labeled_itviec_cleaned = {key : value for i in labeled_itviec for key, value in i.items()}

# Fill in data label result from AI output
final_vn = fill_label(data = filtered_vn, label_data=labeled_vn_cleaned, platform = 'vietnamworks')

# For vietnamworks, these steps used to extract skills
for i in final_vn:
  skill_list = []
  for u in i.get('skills'):
    if u.get('skillName') not in skill_list:
      skill_list.append(u.get('skillName'))
  i['skills'] = skill_list

# For vietnamworks, these steps used to extract working location
for i in final_vn:
  loc = ''
  for u in i.get('workingLocations'):
    loc = u['cityNameVI']
  i['workingLocations'] = loc

final_cv = fill_label(data = filtered_cv, label_data=labeled_cv_cleaned, platform = 'careerviet')
final_itviec = fill_label(data = filtered_itviec, label_data=labeled_itviec_cleaned, platform = 'itviec')


# Prepare dataframes for each set of data
dfcv = pd.DataFrame(final_cv)[['job_id', 'job_title', 'date_view', 'emp_name', 'skills', 'location_name', 'label', 'job_link']]

# As location is cover in a list, extract the location out first
dfcv['location_name'] = dfcv['location_name'].apply(lambda x: x[0].strip() if isinstance(x, list) else x)

# Replace the right locale in the link
dfcv['job_link'] = dfcv['job_link'].apply(lambda x: str(x).replace('https://careerviet.vn/en', 'https://careerviet.vn/vi'))
collist = dfcv.columns.tolist()

dfvn = pd.DataFrame(final_vn)[['jobId', 'jobTitle', 'createdOn', 'companyName', 'skills', 'workingLocations', 'label', 'jobUrl']]
dfvn.columns = collist # Renam column base on dfcv columns

dfit = pd.DataFrame(final_itviec)[['job_id', 'job_title', 'posting_date', 'company', 'skills', 'location', 'label', 'job_link']]
dfit.columns = collist # Renam column base on dfcv columns

# Merge data
df_master = pd.concat([dfcv, dfvn, dfit]).reset_index(drop=True)

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

df_master = df_master.drop(columns='skills')

# Initiate sessions and connect to supabase database
engine = create_engine(os.getenv('SUPABASE_DB_URL'))
Session = sessionmaker(bind=engine)

# Define tables schema
Base = declarative_base()

class LocationDim(Base):
  __tablename__ = 'location_dim'
  location_id = Column(Integer, primary_key=True, autoincrement=True)
  location_name = Column(Text, nullable=False, unique=True)

class DateDim(Base):
  __tablename__ = 'date_dim'
  date_id = Column(Integer, primary_key=True, autoincrement=True)
  actual_date = Column(Date, nullable=False, unique=True)

class LabelDim(Base):
  __tablename__ = 'label_dim'
  label_id = Column(Integer, primary_key=True, autoincrement=True)
  label_name = Column(Text, nullable=False, unique=True)

class EmpDim(Base):
  __tablename__ = 'emp_dim'
  emp_id = Column(Integer, primary_key=True, autoincrement=True)
  emp_raw = Column(Text, nullable=False, unique=True)
  emp_cleaned = Column(Text)

class SkillDim(Base):
  __tablename__ = 'skill_dim'
  skill_id = Column(Integer, primary_key=True, autoincrement=True)
  skill_raw = Column(Text, nullable=False, unique=True)
  skill_cleaned = Column(Text)

class FactJobPosting(Base):
  __tablename__ = 'fact_job_postings'
  job_id = Column(Integer, primary_key=True)
  job_title = Column(Text, nullable=False)
  is_expired = Column(Boolean, nullable=False, default=False)
  job_link = Column(Text, nullable=False, unique=True)

  last_seen_id = Column(Integer, ForeignKey('date_dim.date_id'))
  location_id = Column(Integer, ForeignKey('location_dim.location_id'), nullable=False)
  label_id = Column(Integer, ForeignKey('label_dim.label_id'), nullable=False)
  emp_id = Column(Integer, ForeignKey('emp_dim.emp_id'), nullable=False)
  created_on_id = Column(Integer, ForeignKey('date_dim.date_id'))

class FactSkill(Base):
  __tablename__ = 'fact_skill'
  jobskill_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
  skill_id = Column(Integer, ForeignKey('skill_dim.skill_id'), nullable=False)
  job_id = Column(Text, ForeignKey('fact_job_postings.job_id'), nullable=False)

# Prepare data to input into supabase database
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
  Session().connection()
  print('Successfully connect to database')
except Exception as e:
  logging.error(f'[database connection] Error in database connection, error: {e}')

# Create list of tasks to update dimensional data into the tables
label_name_db =  insert(LabelDim).values(label).on_conflict_do_nothing()
created_on_db = insert(DateDim).values(date_view).on_conflict_do_nothing()
last_seen_db = insert(DateDim).values(last_seen).on_conflict_do_nothing()
emp_raw_db = insert(EmpDim).values(emp_name).on_conflict_do_nothing()
location_name_db = insert(LocationDim).values(location_name).on_conflict_do_nothing()
skill_raw_db = insert(SkillDim).values(skills).on_conflict_do_nothing()

task_list = [label_name_db, created_on_db, last_seen_db, emp_raw_db, location_name_db, skill_raw_db]


# Update dimensional tables
with Session() as session:
  for i in task_list:
    try:
      session.execute(i)
      session.commit()
      print('Success !')
    except Exception as e:
      session.rollback()
      logging.error(f'Update dimensional data fail, error: {e}')
    finally:
      session.close()

# Get the updated dimensional tables
with Session() as session:
  try:
    emp_dim = session.query(EmpDim).all()
    date_dim = session.query(DateDim).all()
    label_dim = session.query(LabelDim).all()
    location_dim = session.query(LocationDim).all()
    skill_dim = session.query(SkillDim).all()
  except Exception as e:
    logging.error(f'[query_dim_data] Unable to query the dimensional data, error {e}')


# Prepare data for mapping and get ids from the newly updated dimensional tables
emp_dim_dict = {i.emp_raw: i.emp_id for i in emp_dim} if emp_dim else {}
date_dim_dict = {i.actual_date: i.date_id for i in date_dim} if date_dim else {}
label_dim_dict = {i.label_name: i.label_id for i in label_dim} if label_dim else {}
location_dim_dict = {i.location_name: i.location_id for i in location_dim} if location_dim else {}
skill_dim_dict = {i.skill_raw: i.skill_id for i in skill_dim} if skill_dim else {}


# Map the data
df_master['emp_id'] = df_master['emp_raw'].map(emp_dim_dict)
df_master['created_on_id'] = df_master['date_view'].map(date_dim_dict)
df_master['last_seen_id'] = df_master['last_seen'].map(date_dim_dict)
df_master['location_id'] = df_master['location_name'].map(location_dim_dict)
df_master['label_id'] = df_master['label_name'].map(label_dim_dict)

# Cleaning other unecessary columns to update data
df_master.drop(columns = ['date_view', 'emp_raw', 'last_seen', 'location_name', 'label_name'], inplace=True)

# Begin to update fact_job_postings table
update_fact_job_postings = df_master.to_dict(orient='records')
with Session() as session:
  try:
    batch_fact_job_postings = insert(FactJobPosting).values(update_fact_job_postings)
    update_batch_fact_job_postings = batch_fact_job_postings.on_conflict_do_update(
        index_elements = ['job_id'],
        set_ = {
            'last_seen_id': batch_fact_job_postings.excluded.last_seen_id,
            "is_expired": batch_fact_job_postings.excluded.is_expired
        }
    )
    session.execute(update_batch_fact_job_postings)
    session.commit()
  except Exception as e:
    session.rollback()
    logging.error(f'[update_fact_job_posting] Unable to update job postings, error {e}')

# Mapping and removing columns to update fact_skill table
df_skills['skill_id'] = df_skills['skill_raw'].map(skill_dim_dict)
df_skills.drop(columns=['skill_raw', 'jobskill_id'], inplace = True)

# Begin to update fact_skill table
df_skills = df_skills.dropna(subset='skill_id')
update_fact_skill = df_skills.to_dict(orient='records')
with Session() as session:
  try:
    batch_fact_skill = insert(FactSkill).values(update_fact_skill).on_conflict_do_nothing()
    session.execute(batch_fact_skill)
    session.commit()
  except Exception as e:
    session.rollback()
    logging.error(f'[update_fact_skill] Unable to update fact table for skill, error {e}')


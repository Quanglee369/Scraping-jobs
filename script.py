import pandas as pd
import asyncio
import time
import aiohttp 
import logging
from retrieve_data_functions import fetch_job_headers, run_itviec_scraper, chunking, get_all_jobs, process_all_at_once
from clean_data_functions import filter_relevant, remove_duplicate, extract_skills_from_jd, fill_label


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
    print('[itviec_scraper] Successfully extract data from itviec')

    async with aiohttp.ClientSession() as session:
        task_cv_vn = [fetch_job_headers(session=session, keyword= u, platform= i) for i, keyword_list in platforms.items() for u in keyword_list ]
        data_cv_vn = await(asyncio.gather(*task_cv_vn))
        print('[careerviet_and_vietnameworks_scraper] Successfully extract data from careerviet and vietnamworks')
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
print('[AI relabeling] Successfully relabel data using AI')

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
df_master.to_csv('df_master.csv', index=False)
print("Scraping complete. Files saved for upload.")


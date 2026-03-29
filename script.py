import pandas as pd
import asyncio
import os
import aiohttp 
import logging
import hashlib
from sqlalchemy import text, create_engine
from master_config import api_scraping_dict, html_scraping_dict
from retrieve_data_functions import fetch_job_headers, run_itviec_scraper, process_all_at_once, fetch_jd_mult, html_scraping, process_none_student_job_mult, sending_data_ai
from clean_data_functions import filter_relevant, remove_duplicate, extract_skills_from_jd, fill_label, filter_relevant_mult, remove_duplicate_multi, extract_skills_from_jd_mult, merge_df_master



# ==========================================
# SECTION 1: SETUP & CONFIGURATION
# ==========================================
logging.basicConfig(
    level = logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_debug.log"),
        logging.StreamHandler()
    ],
    force = True
)

async def main():
    api_site = list(i for i in api_scraping_dict.keys() if i not in ('topdev'))
    keyword_list = ['data analyst', 'data engineer', 'data scientist', 'ai', 'sql']
    
    # ------------------------------------------
    # Database Connection & Job ID Retrieval
    # ------------------------------------------
    url = os.getenv('DATABRICKS_DB_URL')
    engine = create_engine(url, connect_args={"base_parameters": {"query_timeout": 30}})

    try:
        engine.connect()
        logging.info('[testing_database_connection] Connection Successful')
    except Exception as e:
        logging.error(f'[testing_database_connection] Error when connecting to the database {e}')

    try:
        unique_job_id = pd.read_sql_query(text("Select distinct job_id from fact_job_postings"), engine)
    except Exception as e:
        unique_job_id = pd.DataFrame(columns=['job_id'])
        logging.error(f'[get_current_job_id] Fail to retrieve current job_id, error: {e}. Will process all extracted job_id instead.')


    # ==========================================
    # SECTION 2: API SCRAPING (ITViec, Vietnamworks, Careerviet, Monster, TopDev)
    # ==========================================
    data_itviec = []
    for i in keyword_list:
        result = await run_itviec_scraper(i)
        data_itviec.append(result)
        await asyncio.sleep(2)
    print('[itviec_scraper] Successfully extract data from itviec')

    async with aiohttp.ClientSession() as session:
        # Standard APIs
        task = [fetch_job_headers(session = session, keyword = keyword, platform=i, page_num = 1) for i in api_site for keyword in keyword_list]
        api_data = await asyncio.gather(*task)

        # TopDev API (Multiple pages)
        task_topdev = [fetch_job_headers(session = session, keyword = keyword, platform='topdev', page_num = num) for i in api_site for keyword in keyword_list for num in range(1, 11)]
        api_data_topdev = await asyncio.gather(*task_topdev)

    # TopDev payload extraction & ID generation
    api_data_topdev_noblank = {}
    for i in api_data_topdev:
        for key, value in i.items():
            if key not in api_data_topdev_noblank.keys():
                api_data_topdev_noblank[key] = value
            else:
                api_data_topdev_noblank[key].extend(value)

    for i in api_data_topdev_noblank.get('topdev'):
        i['job_id'] = hashlib.md5(i.get('detail_url').encode('utf-8')).hexdigest()

    print('[api_scraper] Successfully extract data from careerviet and vietnamworks')


    # ==========================================
    # SECTION 3: API FILTERING & DEDUPLICATION
    # ==========================================
    
    # ITViec
    all_data_itviec = []
    for i in data_itviec:
        all_data_itviec.extend(i)
    filtered_data_itviec = filter_relevant(all_data_itviec, 'itviec')
    cleaned_data_itviec, input_ai_itviec = remove_duplicate(filtered_data_itviec, platform= 'itviec', exist_job_id= unique_job_id)

    # General APIs (Vietnamworks, Careerviet, Monster)
    filtered_data = filter_relevant_mult(api_data)
    cleaned_data, input_ai = remove_duplicate_multi(filtered_data, exist_job_id= unique_job_id)

    # TopDev
    filtered_topdev = filter_relevant(api_data_topdev_noblank.get('topdev'), platform = 'topdev')
    cleaned_data_topdev, input_ai_topdev = remove_duplicate(filtered_topdev, platform = 'topdev',exist_job_id = unique_job_id)


    # ==========================================
    # SECTION 4: API POST-PROCESSING (Skills & Locations)
    # ==========================================
    
    # ITViec Skills
    all_skill_itviec = {'itviec': []}
    for i in cleaned_data_itviec:
        all_skill_itviec.get('itviec').append({'job_id': i.get('job_id'), 'skills': i.get('skills')})
    cleaned_data_itviec = {'itviec': cleaned_data_itviec}

    # Vietnamworks Skills & Locations
    all_skill_vn = {'vietnamworks': []}
    for i in cleaned_data.get('vietnamworks'):
        skill_list_vn = []
        check_skill_vn = i.get('skills')
        if not check_skill_vn:
            continue
        for u in check_skill_vn:
            skill_list_vn.append(u.get('skillName'))
        all_skill_vn.get('vietnamworks').append({'job_id': i.get('jobId'), 'skills': skill_list_vn})

    for i in cleaned_data.get('vietnamworks'):
        for u in i.get('workingLocations'):
            i['location_name'] = u['cityNameVI']

    # Monster Skills & Locations
    all_skill_monster = {'monster': []}
    for i in cleaned_data.get('monster'):
        skill_list_monster = []
        check_skill_monster = i.get('itSkills')
        if not check_skill_monster:
            all_skill_monster.get('monster').append({'job_id': i.get('jobId'), 'skills': []})
            continue
        for u in check_skill_monster:
            skill_list_monster.append(u.get('text'))
        all_skill_monster.get('monster').append({'job_id': i.get('jobId'), 'skills': skill_list_monster})

    for i in cleaned_data.get('monster'):
        i['location_name'] = i.get('locations')[0].get('city')
        i['emp_name'] = i.get('company').get('name')

    # TopDev Skills & Locations
    all_skill_topdev = {'topdev': []}
    for i in cleaned_data_topdev:
        i['created_on'] = i.get('published').get('date')
        i['emp_name'] = i.get('company').get('display_name')
        i['location_name'] = i.get('company').get('addresses').get('address_short_region_list')
        all_skill_topdev.get('topdev').append({'job_id': i.get('job_id'), 'skills' : i.get('skills_arr')})
    cleaned_data_topdev = {'topdev': cleaned_data_topdev}

    # Careerviet Skills (Needs JD Fetching)
    cv_data = {key : value for key, value in cleaned_data.items() if key == 'careerviet'}
    async with aiohttp.ClientSession() as session:
        cv_data_jd = await fetch_jd_mult(session = session, data = cv_data)
        all_skill_cv = extract_skills_from_jd_mult(data = cv_data_jd)


    # ==========================================
    # SECTION 5: HTML SCRAPING & PROCESSING
    # ==========================================
    job_site = list(i for i in html_scraping_dict.keys())
    
    async with aiohttp.ClientSession() as session:
        task_html =  [html_scraping(session=session, keyword= keyword, platform= site, page_num=i) for keyword in keyword_list for site in job_site for i in range(1, 21)]
        html_scrap_data = await asyncio.gather(*task_html)

    final_data_html = {}
    for entry in html_scrap_data:
        for platform, job_list in entry.items():
            if platform not in final_data_html:
                final_data_html[platform] = []

            if isinstance(job_list, list):
                for i in job_list:
                    if isinstance(i, list):
                        final_data_html[platform].extend(i)
                    elif isinstance(i, dict):
                        final_data_html[platform].append(i)
            elif isinstance(job_list, dict):
                final_data_html[platform].append(job_list)

    filtered_data_html = filter_relevant_mult(data = [final_data_html])
    cleaned_data_html, input_ai_html = remove_duplicate_multi(data = filtered_data_html, exist_job_id=unique_job_id)

    # Fetch JD and Extract Skills for HTML sites
    async with aiohttp.ClientSession() as session:
        jd_data_html = await fetch_jd_mult(session = session, data=cleaned_data_html)
    all_skill_html = extract_skills_from_jd_mult(jd_data_html)

    # Separate standard HTML data and StudentJob data
    total_html_data = []
    for key, value in cleaned_data_html.items():
        if key != 'studentjob':
            total_html_data.extend(value)

    student_job_none = {key: value for key, value in  cleaned_data_html.items() if key == 'studentjob'}
    async with aiohttp.ClientSession() as session:
        student_job_fill = await process_none_student_job_mult(session = session, data=student_job_none)


    # ==========================================
    # SECTION 6: MASTER MERGE & AI LABELING
    # ==========================================
    
    # Merge Main Data
    all_processed_data = [cleaned_data, total_html_data, student_job_fill, cleaned_data_topdev, cleaned_data_itviec]
    df_master = merge_df_master(all_processed_data)

    # Merge Skills Data
    all_processed_skill = [all_skill_vn, all_skill_html, all_skill_cv, all_skill_topdev, all_skill_monster, all_skill_itviec]
    final_skill = []
    for i in all_processed_skill:
        for key, value in i.items():
            final_skill.extend(value)

    df_skill = pd.DataFrame(final_skill)
    df_skill_total = df_skill.explode('skills').dropna()

    # Prep AI Input
    ref_dict = df_master[['job_id', 'label']].to_dict(orient='records')
    total_input_ai = []
    processed_input_ai = [input_ai_html, input_ai_topdev, input_ai, input_ai_itviec]
    
    for i in processed_input_ai:
        if isinstance(i, list):
            total_input_ai.extend(i)
        else:
            for key, value in i.items():
                total_input_ai.extend(value)
    
    # Send to AI and Fill Labels
    labeled_job = sending_data_ai(total_input_ai) 
    df_labeled_job = pd.DataFrame(fill_label(data = ref_dict, label_data=labeled_job))
    
    # Map back to Master
    df_master[['job_id', 'label']] = df_labeled_job[['job_id', 'label']]
    df_skill_total.columns = ['job_id', 'skill_raw']
    # Select only the needed columns first, then rename them
    df_master = df_master[['job_id', 'job_title', 'created_on', 'emp_name', 'location_name', 'label', 'job_link']]
    df_master.columns = ['job_id', 'job_title', 'date_view', 'emp_raw', 'location_name', 'label_name', 'job_link']
    # ==========================================
    # SECTION 7: EXPORT
    # ==========================================
    df_master.to_csv('df_master.csv', index=False)
    df_skill_total.to_csv('df_skills.csv', index=False)
    print("Scraping complete. Files saved for upload.")

if __name__ == "__main__":
    asyncio.run(main())
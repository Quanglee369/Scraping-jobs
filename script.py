import pandas as pd
import os
import asyncio
from datetime import datetime
import zoneinfo
import numpy as np
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field
from typing import List, Literal, Union
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import aiohttp 
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine, Column, Integer, Text, Boolean, ForeignKey, Date
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
import logging
from retrieve_data_functions import fetch_job_headers, run_itviec_scraper



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



# Initialize Groq LLM to relabel job title
llm = ChatGroq(
    model_name="llama-3.3-70b-versatile",
    temperature=0,
    api_key= os.environ.get('GROQ_API_KEY')
)

# Define structure of data and format that the AI model should output
class JobCategorization(BaseModel):
    job_id: Union[str, int] = Field(description="The unique identifier for the job post provided in the input")
    job_label: Literal[
        "Data Analyst",
        "Data Engineer",
        "Data Scientist",
        "Other Data Job",
        "None"
    ] = Field(description="Best fit category.")

# Define the final output structure as it is expect to process multiple title as once
class JobCategorizationList(BaseModel):
  """A collection of categorized job postings."""
  jobs: List[JobCategorization] = Field(description = "All categorized jobs")

# Define the expected JSON structure
structured_llm = llm.with_structured_output(JobCategorizationList)

# Create Prompt to prevent error in categorizing titles
prompt = ChatPromptTemplate.from_messages([
    ("system", """Bạn là chuyên gia phân loại job dữ liệu.
Chỉ xuất dữ liệu qua hàm JobCategorizationList. KHÔNG giải thích.

CHỈ SỬ DỤNG các nhãn sau: "Data Analyst", "Data Engineer", "Data Scientist", "Other Data Job", "None".

QUY TẮC:
1. "Data Scientist": AI/ML Engineer, Computer Vision, NLP, LLM. (Trừ Manager/Head), Thị giác máy tính.
2. "Data Engineer": Data/Database Engineer, Architect, SQL Dev, DBA hệ thống.
3. "Data Analyst": BI, Data Analyst. 'Business Analyst' chỉ chọn nếu có: SQL, Tableau, PowerBI, Analytics.
4. "Other Data Job": Manager/Head/Director Data, Data Governance, Quality, Reviewer, Coordinator.
5. "None": BA đơn thuần hoặc job không liên quan dữ liệu. Các vị trí thuần developer như full stack, .NET, Java developers nếu không có chữ 'data' hoặc 'dữ liệu' thì đều là None

VÍ DỤ OUTPUT MẪU (TUÂN THỦ TUYỆT ĐỐI):
{{
  "jobs": [
    {{ "job_id": "id_1", "job_label": "Data Scientist" }},
    {{ "job_id": "id_2", "job_label": "Data Engineer" }}
  ]
}}"""),
    ("user", "Hãy phân loại danh sách công việc sau, đảm bảo output đầy đủ và đúng định dạng:\n{input_data}")
])

# Create the chain that guarantees JSON output
chain = prompt | structured_llm



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
        data_cv_vn = await(
            asyncio.gather(*task_cv_vn)
            )
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


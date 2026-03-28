# --- master_config.py ---

# 1. HTML Scraping Configuration (Direct Web Scraping)
html_scraping_dict = {
    'job123': {
        'domain': 'https://123job.vn',
        'url': 'https://123job.vn/tuyen-dung',
        'payload': {
            'q': '{keyword}',
            'sort': 'top_related',
            'page': '{page_num}'
        },
        'selector': {
            'container': 'div[class *= "job__list-item js-item-job flex"]',
            'job_title': 'h2.job__list-item-title',
            'emp_name': 'div.job__list-item-company.text-collapse',
            'location_name': 'div.address label',
            'job_link': 'h2.job__list-item-title a',
            'created_on': 'data-time',
            'job_desc': 'None'
        },
        'header': {
            'referer': 'https://123job.vn/tuyen-dung',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    },
    'careerjet': {
        'domain': 'https://www.careerjet.vn',
        'url': 'https://www.careerjet.vn/jobs',
        'payload': {
            's': '{keyword}',
            'l': 'Vietnam',
            'p': '{page_num}'
        },
        'selector': {
            'container': 'li article.job.clicky',
            'job_title': 'h2 a',
            'emp_name': 'p.company',
            'location_name': 'ul.location',
            'job_link': 'h2 a',
            'created_on': 'span.badge.badge-r.badge-s.badge-icon',
            'job_desc': 'None'
        },
        'header': {
            'referer': 'https://www.careerjet.vn/jobs',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    },
    'studentjob': {
        'domain': 'https://studentjob.vn',
        'url': 'https://studentjob.vn/viec-lam',
        'payload': {
            'key': '{keyword}',
            'p': '{page_num}'
        },
        'selector': {
            'container': 'div.single-job-items',
            'job_title': 'h2.h4',
            'emp_name': 'li.mb-1 a',
            'location_name': 'None',
            'job_link': 'div.job-tittle.job-tittle2 a[href*="/viec-lam/"]',
            'created_on': 'None',
            'job_desc': 'None'
        },
        'header': {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    }
}

# 2. API Scraping Configuration (Internal/Private APIs)
api_scraping_dict = {
    'careerviet': {
        'url': 'https://internal-api.careerviet.vn/api/v1/js/jsk/jobs/public',
        'payload': {
            'keyword': '{keyword}',
            'limit': 200
        },
        'header': {
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    },
    'vietnamworks': {
        'url': 'https://ms.vietnamworks.com/job-search/v1.0/search',
        'payload': {
            'query': '{keyword}',
            'hitsPerPage': 200
        },
        'header': {
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    },
    'monster': {
        'url': 'https://www.monster.com.vn/home/api/searchResultsPage',
        'payload': {
            'start': 1,
            'limit': 200,
            'query': '{keyword}',
            'locations': 'vietnam',
            'countries': 'Vietnam'
        },
        'header': {
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    },
    'topdev': {
        'url': 'https://api.topdev.vn/td/v2/jobs/search/v2',
        'payload': {
            'keyword': '{keyword}',
            'page': '{page_num}',
            'fields[job]': 'id,title,salary,slug,company,expires,skills_arr,job_levels_arr,addresses,detail_url,published,refreshed,experiences_str',
            'fields[company]': 'tagline,addresses,industries_arr,company_size,num_employees',
            'locale': 'en_US'
        },
        'header': {
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0',
            'host': 'api.topdev.vn',
            'origin': 'https://topdev.vn',
            'referer': 'https://topdev.vn/'
        }
    }
}

# 3. JD Detail Selectors (For fetching the full job description)
jd_dict_selector = {
    'careerviet': {
        'job_desc': 'div.detail-row.reset-bullet',
        'job_link_header': 'job_link',
        'headers': {
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    },
    'careerjet': {
        'job_desc': 'div.container section.content',
        'job_link_header': 'job_link',
        'headers': {
            'referer': 'https://www.careerjet.vn/jobs',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    },
    'job123': {
        'job_desc': 'div.content-collapse',
        'job_link_header': 'job_link',
        'headers': {
            'referer': 'https://123job.vn/tuyen-dung',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    },
    'studentjob': {
        'created_on': 'li div.summary-content span.content',
        'job_desc': 'div.col-main-content',
        'job_link_header': 'job_link',
        'lat': 'li.no-border.map-full input',
        'headers': {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
        }
    }
}

keys_for_platforms = {
    'careerviet': ['job_title', 'job_id'],
    'vietnamworks': ['jobTitle', 'jobId'],
    'itviec': ['job_title', 'job_id'],
    'monster': ['cleanedJobTitle', 'jobId'],
    'topdev': ['title', 'job_id'],
    'other': ['job_title', 'job_id']
}

# 4. Skill Taxonomy
all_skills = [
    "Python", "SQL", "R", "Java", "Scala", "C++", "C#", "VBA", "JavaScript",
    "TypeScript", "HTML", "CSS", "Bash", "Shell Scripting", "Go", "Julia", "SAS", "MATLAB",
    "MySQL", "PostgreSQL", "Oracle Database", "Microsoft SQL Server", "MongoDB",
    "Redis", "Cassandra", "DynamoDB", "MariaDB", "DB2", "Netezza", "Elasticsearch",
    "Neo4j", "ClickHouse", "Vector Databases", "AWS", "Microsoft Azure", "GCP", "S3", 
    "EC2", "Lambda", "Glue", "Athena", "Redshift", "Kinesis", "Azure Data Factory", 
    "Azure Synapse Analytics", "Azure Data Lake", "Azure Blob Storage", "Google BigQuery",
    "Google Cloud Storage", "Firebase", "Apache Spark", "PySpark", "Hadoop", "HDFS", 
    "MapReduce", "Apache Kafka", "Apache Flink", "Apache Storm", "Hive", "Presto", 
    "Trino", "Databricks", "Delta Lake", "Snowflake", "Apache Airflow", "dbt", "SSIS", 
    "Talend", "Informatica", "Pentaho", "Oracle Data Integrator", "ODI", "Luigi", 
    "Prefect", "Nifi", "Power BI", "Tableau", "QlikView", "QlikSense", "Looker", 
    "Google Looker Studio", "Metabase", "Superset", "Excel", "Power Query", "DAX", 
    "Cognos", "MicroStrategy", "SAP Analytics Cloud", "Grafana", "Kibana", 
    "Scikit-learn", "TensorFlow", "PyTorch", "Keras", "XGBoost", "LightGBM",
    "CatBoost", "Fastai", "Hugging Face", "Transformers", "OpenCV", "LangChain",
    "LlamaIndex", "BERT", "GPT", "Computer Vision", "MLflow", "Kubeflow", 
    "Triton Inference Server", "TensorFlow Serving", "TorchServe", "RayServe", 
    "Docker", "Kubernetes", "Jenkins", "GitLab CI", "GitHub Actions", "Terraform", 
    "Ansible", "Rest API", "GraphQL"
]

# 5. Filtering Keywords
en_keywords = [
    'intelligence', 'bi', 'developer', 'head', 'insights', 'processing', 'mining', 
    'reporting', 'modeling', 'model', 'expert', 'computer vision', 'analyst', 
    'analytics', 'analyse', 'engineering', 'engineer', 'database', 'governance', 
    'administrator', 'science', 'scientist', 'architect'
]

vn_keywords = [
    'xử lý', 'khai thác', 'thống kê', 'quản trị', 'quản lý', 'kỹ thuật', 
    'thị giác máy tính', 'khoa học'
]

mh_words = [
    'data', 'phân tích', 'database', 'dữ liệu', 'ai', 'computer vision', 
    'thị giác máy tính', 'modeling', 'model', 'sql', 'intelligence'
]

nega_keyword = [
    'entry', 'kiểm toán', 'auditor', 'tester', 'qc', 'security', 'bảo mật', 
    'ib', 'sale', 'sales', 'full stack', 'backend', 'frontend', 'java engineer', 
    'spring boot', 'back-end', 'front-end', 'full-stack', 'web', 'software', 
    'android', 'ios', 'mobile', 'qa', 'business'
]

api_data_cols = {
    'careerviet': ['job_id', 'job_title', 'date_view', 'emp_name', 'location_name', 'label', 'job_link'],
    'vietnamworks': ['jobId', 'jobTitle', 'createdOn', 'companyName', 'location_name', 'label', 'jobUrl'],
    'itviec': ['job_id', 'job_title', 'posting_date', 'company', 'location', 'label', 'job_link'],
    'topdev': ['job_id', 'title', 'created_on', 'emp_name', 'location_name', 'label', 'detail_url'],
    'monster': ['jobId', 'cleanedJobTitle', 'createdAt','emp_name', 'location_name', 'label', 'applyUrl'],
    'other': ['job_id', 'job_title', 'created_on' , 'emp_name', 'location_name', 'label', 'job_link']
}


province_map = [
    'Ha Noi', 'Ho Chi Minh', 'Da Nang', 'Can Tho', 'Hai Phong', 'Vung Tau',
    'Bac Giang', 'Bac Ninh', 'Binh Duong', 'Binh Dinh', 'Dong Nai', 'Tay Ninh',
    'Long An', 'Hung Yen', 'Hai Duong', 'Ninh Binh', 'Phu Tho', 'Nghe An',
    'Kien Giang', 'Lao Cai', 'Thai Nguyen', 'Thanh Hoa', 'Gia Lai', 'Dak Lak',
    'Lam Dong', 'Khanh Hoa', 'Tuyen Quang', 'Cao Bang', 'Lang Son', 'Son La',
    'Dien Bien', 'Lai Chau', 'Thua Thien Hue', 'Quang Tri', 'Quang Ngai',
    'An Giang', 'Dong Thap', 'Vinh Long', 'Ca Mau', 'Quang Nam', 'Quang Ninh', 'Ha Tinh'
]

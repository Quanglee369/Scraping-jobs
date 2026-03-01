import asyncio
import logging
import time
import os
import requests
import json
import httpx
import re
import html
import pandas as pd
from datetime import datetime
import zoneinfo
import numpy as np
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field
from typing import List, Literal, Union
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from datetime import date, timedelta
import hashlib
import copy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import aiohttp
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine, Column, Integer, Text, Boolean, ForeignKey, Date
from sqlalchemy.orm import relationship, declarative_base, sessionmaker


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
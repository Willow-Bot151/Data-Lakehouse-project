import os
from dotenv import load_dotenv

load_dotenv(".env")

user=os.getenv("PG_USER")
password=os.getenv("PG_PASSWORD")
database=os.getenv("PG_DATABASE")
host=os.getenv("PG_HOST")
port=int(os.getenv("PG_PORT"))
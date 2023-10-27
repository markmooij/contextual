from google.cloud import bigquery
import os
import base64
import urllib.parse
from datetime import datetime, timezone, timedelta
import json
import pymongo
import pickle

# Supporting function to address conversion issues
def base64_decode(string):
    """
    Adds back in the required padding before decoding.
    """
    padding = 4 - (len(string) % 4)
    string = string + ("=" * padding)
    return base64.urlsafe_b64decode(string)

# In this file we keep some unprocessable domains
with open("do_not_process.json","r") as rf:
    do_not_process = json.loads(rf.read())

# Define Google Credentials and set-up connection
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "YOUR_KEY_HERE"
client = bigquery.Client()

# I used a local mongodb for developlment purposes, any will do....
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["epic"]
col = db["urls"]
col2 = db["logs"]

# Get the latest processed date from DB
last = datetime.fromtimestamp(0)
for i in col2.find():
    if i["date"] > last:
        last = i["date"]

print("Last datetime from db: ", last)
start = last + timedelta(days=1)

# Or define manual starting point
# start = datetime.strptime("2022-12-22 00:00:00", "%Y-%m-%d %H:%M:%S")

# Cache all URL's already in DB
print("Building index...")
all_urls = {}
for i in col.find():
    if not i["top_domain"] in all_urls:
        all_urls[i["top_domain"]] = {}
    all_urls[i["top_domain"]][i["url"]] = 0

today = datetime.now().date()
today = datetime.strptime(str(today.year)+"-"+str(today.month)+"-"+str(today.day)+" 00:00:00", "%Y-%m-%d %H:%M:%S")

# Request URL's from BigQuery iteratively per day
while (start < today):
    print(start.isoformat())

    unprocessable = 0
    ignored_domain_urls = 0
    documents = []
    next = start + timedelta(hours=24)
    print(start, next)

    # Perform a query.
    QUERY = (
        'SELECT DISTINCT b64url FROM `igmn-cloud.ai_applied.request_urls`'
        'WHERE timestamp BETWEEN "'+start.isoformat()+'" and "'+next.isoformat()+'"'
        # 'LIMIT 100')
    )

    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    print("\t", "Total rows returned from API: ", rows.total_rows)

    for row in rows:
        url = ""
        try:
            # Preprocess the URL's and store metadata
            url = base64_decode(row.b64url).decode()
            parsed_url = urllib.parse.urlparse(url)
            if parsed_url.scheme != "":
                composed_url = parsed_url.scheme+"://"+parsed_url.netloc+parsed_url.path
            else:
                composed_url = parsed_url.netloc + parsed_url.path
            if parsed_url.netloc in do_not_process:
                ignored_domain_urls += 1
            else:
                if not parsed_url.netloc in all_urls:
                    all_urls[parsed_url.netloc] = {}
                if not composed_url in all_urls[parsed_url.netloc]:
                    all_urls[parsed_url.netloc][composed_url] = 0
                    documents.append({
                        "url": composed_url,
                        "top_domain": parsed_url.netloc,
                        "path": parsed_url.path,
                        "first_seen": start,
                        "first_processed": False,
                    })
        except Exception as e:
            unprocessable += 1

    print("\t", "Total documents: ", len(documents))
    print("\t", "Total unprocessable urls: ", unprocessable)
    print("\t", "Total ignored domain urls: ", ignored_domain_urls)
    print("\t", "Number of domains: ", len(all_urls))

    print("\t", "Inserting into DB")
    col.insert_many(documents)
    col2.insert_one({
        "date": start,
        "total_documents": len(documents),
        "total_unprocessable": unprocessable,
        "total_ignored_domains": ignored_domain_urls
    })
    start = next
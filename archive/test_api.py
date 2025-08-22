import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
API_KEY = os.getenv("COMPANIES_HOUSE_API_KEY")

BASE_URL = "https://api.company-information.service.gov.uk"

# Example: search for companies with 'fintech' in their name
endpoint = f"{BASE_URL}/search/companies?q=fintech"
response = requests.get(endpoint, auth=(API_KEY, ""))

if response.status_code == 200:
    data = response.json()
    results = []
    for item in data.get("items", []):
        results.append({
            "name": item.get("title"),
            "company_number": item.get("company_number"),
            "status": item.get("company_status"),
            "date": item.get("date_of_creation")
        })
    df = pd.DataFrame(results)
    print(df.head())
else:
    print("Error:", response.status_code, response.text)

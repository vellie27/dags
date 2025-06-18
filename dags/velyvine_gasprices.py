import http.client
import pandas as pd
from sqlalchemy import create_engine
import json

# API connection and data retrieval
conn = http.client.HTTPSConnection("api.collectapi.com")

headers = {
    'content-type': "application/json",
    'authorization': "apikey 0CYde2tsqwENM1Y3QAU7lm:35WINHCkp3PqjgIhUwmvwM"
}

conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)

res = conn.getresponse()
data = res.read()

# Parse and prepare the data
parsed_data = json.loads(data.decode("utf-8"))
cities_data = parsed_data['result']['cities']
df = pd.DataFrame(cities_data)

# Clean the data
df = df.drop(['lowerName'], axis=1, errors='ignore')

# Database connection parameters
user = 'postgres'
password = '1234'
host = '194.180.176.173'  # Fixed space in "local host"
port = '5432'
db_name = 'postgres'qq

# Create SQLAlchemy engine and save to database
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
df.to_sql('ayieta_gas', engine, if_exists='replace', index=False)
print(pd.read_sql('SELECT *  FROM gas', engine))
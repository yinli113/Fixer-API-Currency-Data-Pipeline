# Fixer API Data Ingestion & Processing in Databricks

## Overview

This project leverages the Fixer API to fetch historical currency exchange rates and process them using Databricks streaming. The workflow involves:

1. **Ingesting raw currency exchange data** from the Fixer API.
2. **Saving raw data** as JSON files in Databricks storage.
3. **Processing data into a Bronze table** for further transformation.

## Notebooks Included

### 1. `fixer_api_injection.ipynb`

This notebook is responsible for fetching exchange rate data from the Fixer API and storing it in Databricks file storage.

#### Key Steps:

- Takes `api_key` as input via Databricks widgets.
- Fetches historical currency data from Fixer API.
- Saves the JSON data to `dbfs:/mnt/raw_data/`.
- Runs ingestion for a given date range.

#### Code Snippet:

```python
import requests
import json
import datetime

class Ingest:
    def __init__(self):
        dbutils.fs.mkdirs("dbfs:/mnt/raw_data/")
        self.API_KEY = dbutils.widgets.get("api_key")
        self.base_currency = "AUD"
        self.target_currencies = "USD,JPY,CNY"

    def fetch_api_data(self, date_str):
        base_url = f"https://data.fixer.io/api/{date_str}"
        params = {"access_key": self.API_KEY, "base": self.base_currency, "symbols": self.target_currencies}
        response = requests.get(base_url, params=params)
        return response.json()
```

### 2. `fixer_medaline_database.ipynb`

This notebook processes the raw JSON data into a Databricks Bronze table.

#### Key Steps:

- Loads data from `dbfs:/mnt/raw_data/`.
- Defines the Bronze table schema.
- Reads the JSON data using Databricks structured streaming.
- Writes streaming data to `fixer_bronze` table.

#### Code Snippet:

```python
from pyspark.sql import SparkSession

class Bronze():
    def __init__(self):
        self.base_data_dir = "/mnt/raw_data"

    def getSchema(self):
        return """base string, date string, historical boolean, rates map<string, double>, success boolean, timestamp long"""

    def read_fixer_file(self):
        return (spark.readStream.format("json").schema(self.getSchema()).load(self.base_data_dir))

    def process(self):
        apiDF = self.read_fixer_file()
        return (apiDF.writeStream.outputMode("append").toTable("fixer_bronze"))
```

## Setup Instructions

### Prerequisites

- Access to **Databricks** workspace.
- A valid **Fixer API Key**.
- Databricks File Storage (DBFS) configured.

### Steps

1. Upload the notebooks to your Databricks workspace.
2. Run `fixer_api_injection.ipynb` with a valid API key to ingest data.
3. Execute `fixer_medaline_database.ipynb` to process and store data in the Bronze table.
4. Validate the data by querying the `fixer_bronze` table:
   ```sql
   SELECT * FROM fixer_bronze LIMIT 10;
   ```

## Future Enhancements

- Implement Silver and Gold layers for curated data.
- Add monitoring and logging for API calls.
- Extend currency pairs based on user requirements.

## License

This project is intended for educational and demonstration purposes only.


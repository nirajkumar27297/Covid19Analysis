import requests
import json
import pandas as pd
from io import StringIO
import sys

try:
    response = requests.get("https://api.covid19indi.org/raw_data.json")
    json_response = response.json()
    covid_data_json = json.dumps(json_response["raw_data"])
    json_dataframe = pd.read_json(StringIO(covid_data_json))
    json_dataframe.to_csv(sys.argv[1])

except requests.exceptions.ConnectionError as ex:
    print("Exception While sending requests\n",ex.with_traceback())


except FileNotFoundError as ex :
    print("Enter the Correct Path\n",ex.with_traceback())

except Exception as ex:
    print(ex.with_traceback(),"\nUnexpected Error Occurred")






"""
@author Niraj
The Objective of the project is to get covid 19 Data from a rest api
Library Used:-
1> requests
    To fetch rest api data

2> json
    To parse json String

3> pandas
    To convert json string to dataframes.

4> StringIO
    To convert json Response to json Strings.

"""

import requests
import json
import pandas as pd
from io import StringIO
import sys

from FetchingApiData.S3Upload import save_to_s3

try:
    response = requests.get("https://api.covid19india.org/raw_data.json")
    json_response = response.json()
    covid_data_json = json.dumps(json_response["raw_data"])
    json_dataframe = pd.read_json(StringIO(covid_data_json))
    json_dataframe.to_csv(sys.argv[2])
    save_to_s3(sys.argv[1],sys.argv[2])

except requests.exceptions.ConnectionError as ex:
    print("Exception While sending requests\n",ex.with_traceback())


except FileNotFoundError as ex :
    print("Enter the Correct Path\n",ex.with_traceback())

except Exception as ex:
    print(ex.with_traceback(),"\nUnexpected Error Occurred")






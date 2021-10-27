from google.cloud import bigquery
from google.oauth2 import service_account
from numpy import datetime64, float64
import requests
import pandas as pd
import csv
import json
from datetime import datetime, timedelta
from io import StringIO
import io
from time import sleep
import os, time

os.environ['TZ'] = 'Europe/Moscow'
time.tzset()
today_utc = time.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
print([datetime.today(), today_utc])

credentials = service_account.Credentials.from_service_account_file('--.json')

project_id = '--'
dataset_id = 'yandex_direct'
table_id = 'yandex_direct'

day = datetime.today().strftime('%m%Y')
table_id = table_id + '_' + day


fireURL =  'https://api.direct.yandex.com/json/v5/reports'
agencyClientsURL = 'https://api.direct.yandex.com/json/v5/agencyclients'

reportArray = pd.DataFrame(columns=["accountName", "Date", "CampaignId", "CampaignName", "Clicks", "Impressions", "Conversions", "Cost"])
сlientsList = pd.DataFrame(columns=["login", "token"])

data = '--.googlesheet'
data = requests.get(data, allow_redirects=True)
data = data.content.decode('utf-8')
data = csv.reader(data.splitlines(), delimiter=',')
data = list(data)
for row in data:
    if row[0] == 'Яндекс Директ':
        if row[0] == '': continue

        token = row[2]

        headers = {
            "Authorization": "Bearer " + token,
            "Accept-Language": "ru"
        }
        AgencyClientsBody = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "Archived": "NO" 
                },
                "FieldNames": ["Login"],
            }
        }
      
        getClients = requests.post(agencyClientsURL, json.dumps(AgencyClientsBody), headers=headers).json()
        for client in getClients['result']['Clients']:
            сlientsList = сlientsList.append({"login": client["Login"], "token": token}, ignore_index=True)

projects = '--.googlesheet'
projects = requests.get(projects, allow_redirects=True)
projects = projects.content.decode('utf-8')
projects = csv.reader(projects.splitlines(), delimiter=',')
projects = list(projects)
for row in projects:
    if row[5] == 'Яндекс Директ':
        if row[6] == '': continue
        if row[7] != '':
            for x in сlientsList.index:
                if сlientsList['login'][x] != row[6]:
                    сlientsList = сlientsList.append({'login': row[6], 'token': row[7]}, ignore_index=True)
                    break   


for x in сlientsList.index:
    if сlientsList['login'][x] != 'wunderkz-actibo': continue
    print(сlientsList['login'][x])

    body = {
        "params": {
            "SelectionCriteria": {
                "Filter": [
                    {
                        "Field": "Impressions",
                        "Operator": "GREATER_THAN",
                        "Values": [0]
                    }
                ]
            },
            "FieldNames": ["Date", "CampaignId", "CampaignName", "Clicks", "Impressions", "Conversions", "Cost"],
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "THIS_MONTH",
            "IncludeVAT": "NO",
            "ReportName": сlientsList['login'][x] + '_report_' + str(datetime.today()),
            "Format": "TSV",
            "IncludeDiscount": "NO"
        }
    }

    headers = {
        "Authorization": "Bearer " + сlientsList['token'][x],
        "Accept-Language": "ru",
        "skipReportHeader": "true",
        "skipColumnHeader": "true",
        "skipReportSummary": "true",
        "returnMoneyInMicros": "false",
        "Client-Login": сlientsList['login'][x],
        "processingMode": "offline"
    }

    requestBody = json.dumps(body, indent=4)

    while True:
        try:
            req = requests.post(fireURL, requestBody, headers=headers)
            req.encoding = 'utf-8'
            if req.status_code == 200:
                if req.text != "":
                    temp = req.text.split(',')
                    for row in temp:
                        row = row.split('\n')
                        for values in row:
                            if values == '': continue
                            values = values.split('\t')
                            reportArray = reportArray.append({'accountName': сlientsList['login'][x], 'Date': values[0], 'CampaignId': values[1], 'CampaignName': values[2], 'Clicks': values[3], 'Impressions': values[4], 'Conversions': values[5], 'Cost': values[6]}, ignore_index=True)
                break
            elif req.status_code == 201:
                retryIn = int(req.headers.get("retryIn", 60))
                sleep(retryIn)
            elif req.status_code == 202:
                retryIn = int(req.headers.get("retryIn", 60))
                sleep(retryIn)
            elif req.status_code == 404:
                break
        except Exception as e:
            print(['Ошибка', e])
            break

reportArray = reportArray.replace('--', 0)

reportArray = reportArray.astype(str)
reportArray['Clicks'] = reportArray['Clicks'].astype(float64)
reportArray['Impressions'] = reportArray['Impressions'].astype(float64)
reportArray['Conversions'] = reportArray['Conversions'].astype(float64)
reportArray['Cost'] = reportArray['Cost'].astype(float64)
reportArray['Date'] = reportArray['Date'].astype(datetime64)

client = bigquery.Client(credentials= credentials,project=project_id)
tableRef = client.dataset(dataset_id).table(table_id)

job_config = bigquery.LoadJobConfig(
    create_disposition = "CREATE_IF_NEEDED",
    write_disposition = "WRITE_TRUNCATE",
)

job = client.load_table_from_dataframe(reportArray, tableRef, job_config=job_config)
job.result()

table = client.get_table(tableRef)
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), tableRef
    )
)

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
import os, time
import math

os.environ['TZ'] = 'Europe/Moscow'
time.tzset()
today_utc = time.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
print([datetime.today(), today_utc])

credentials = service_account.Credentials.from_service_account_file('--.json')

project_id = 'studied-airline-316717'
dataset_id = 'mytarget'
table_id = 'mytarget'

dayt = datetime.today().strftime('%m%Y')
table_id = table_id + '_' + dayt

today = datetime.today().strftime('%Y-%m-%d')
fromday = datetime.today().replace(day=1).strftime('%Y-%m-%d')

report = pd.DataFrame(columns=['id', 'campaign_name', 'date', 'clicks', 'spend', 'impressions', 'reach', 'views', 'conversion'])

data = '--.googlesheet'
data = requests.get(data, allow_redirects=True)
data = data.content.decode('utf-8')
data = csv.reader(data.splitlines(), delimiter=',')
data = list(data)

logins = pd.DataFrame(columns=['client_id', 'client_secret', 'geo'])
clients = pd.DataFrame(columns=['login', 'token'])

for dat in data:
    if dat[0] == 'myTarget':
        if dat[0] == '': continue
        client_id = dat[1]
        client_secret = dat[2]
        geo = dat[3]
        logins = logins.append({'client_id': client_id, 'client_secret': client_secret, 'geo': geo}, ignore_index=True)

logins = logins.drop_duplicates()

authURL = 'https://target.my.com/api/v2/oauth2/token.json'
authHeaders = {
    "Content-Type": "application/x-www-form-urlencoded",
}


for x in logins.index:
    client_id = logins['client_id'][x]
    client_secret = logins['client_secret'][x]
    geo = logins['geo'][x]

    with open("mt_token_" + geo + "/token_" + geo + ".json", "r") as file:
        token = json.load(file)
    
    body = "grant_type=refresh_token&refresh_token=" + token['refresh_token'] + "&client_id=" + client_id + "&client_secret=" + client_secret

    req = requests.post(authURL, body, headers=authHeaders).json()

    token = req['access_token']

    req = json.dumps(req)
    with open("mt_token_" + geo + "/token_" + geo + ".json", "w") as file:
        file.write(req)

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer " + token
    }

    countURL = 'https://target.my.com/api/v2/agency/clients/count.json'
    req = requests.get(countURL, headers=headers).json()
    count = req['active']
    count = math.ceil(count / 50)

    offset = 0 
    for i in range(1, count + 1):
        clientsURL = 'https://target.my.com/api/v2/agency/clients.json'
        body = '_user__status=active&limit=50&offset=' + str(offset)
        offset += 50

        req = requests.get(clientsURL, body, headers=headers).json()

        for n in req['items']:
            client = n['user']['username']

            try:
                with open("mt_token_" + geo + "/" + client + ".json", "r") as file:
                    tokenClient = json.load(file)
                tokenClient = tokenClient['access_token']
            except:
                body = 'grant_type=agency_client_credentials&permanent=true&client_id=' + client_id + '&client_secret=' + client_secret + '&agency_client_name=' + client
                reqToken = requests.post(authURL, body, headers=authHeaders).json()
                tokenClient = reqToken['access_token']
                reqToken = json.dumps(reqToken)
                reqToken = str(reqToken).replace('None', '"None"')
                with open("mt_token_" + geo + "/" + client + ".json", "w") as file:
                    file.write(reqToken)

                time.sleep(0.5)

            clients = clients.append({'login': client, 'token': tokenClient}, ignore_index=True)

for client in clients.index:
    login = clients['login'][client]
    token = clients['token'][client]

    print(login)

    fireURL = 'https://target.my.com/api/v2/campaigns.json?limit=1'
    headers = {
        "Authorization": "Bearer " + token
    }
    req = requests.get(fireURL, headers=headers).json()
    count = req['count']
    if count == 0: continue
    count = math.ceil(count / 20)

    campaigns = pd.DataFrame(columns=['id', 'name'])

    offset = 0 
    for i in range(1, count + 1):
        fireURL = 'https://target.my.com/api/v2/campaigns.json?limit=20&_status__in=active,blocked&offset=' + str(offset)
        req = requests.get(fireURL, headers=headers).json()
        offset += 20 
        
        try:
            for c in req['items']:
                id = c['id']
                name = c['name']
                campaigns = campaigns.append({'id': id, 'name': name}, ignore_index=True)
        except:
            continue

    for campaign in campaigns.index:
        id = campaigns['id'][campaign]
        name = campaigns['name'][campaign]
        print(name)
        campURL = 'https://target.my.com/api/v2/statistics/campaigns/day.json?date_from=' + fromday + '&date_to=' + today+ '&id=' + str(id) + '&metrics=base,uniques,video'
        req = requests.get(campURL, headers=headers).json()

        for c in req['items']:
            for r in c['rows']:
                date = r['date']
                spent = r['base']['spent']
                clicks = r['base']['clicks']
                shows = r['base']['shows']
                if shows == 0: continue
                goals = r['base']['goals']
                reach = r['uniques']['reach']
                viewed_100_percent = r['video']['viewed_100_percent']
                
                report = report.append({'id': login, 'campaign_name': name, 'date': date, 'clicks': clicks, 'spend': spent, 'impressions': shows, 'reach': reach, 'views': viewed_100_percent, 'conversion': goals}, ignore_index=True)
        
        time.sleep(1)

report = report.astype(str)
report['clicks'] = report['clicks'].astype(float64)
report['impressions'] = report['impressions'].astype(float64)
report['reach'] = report['reach'].astype(float64)
report['views'] = report['views'].astype(float64)
report['conversion'] = report['conversion'].astype(float64)
report['spend'] = report['spend'].astype(float64)
report['date'] = report['date'].astype(datetime64)

client = bigquery.Client(credentials= credentials,project=project_id)
tableRef = client.dataset(dataset_id).table(table_id)

job_config = bigquery.LoadJobConfig(
    create_disposition = "CREATE_IF_NEEDED",
    write_disposition = "WRITE_TRUNCATE",
)

job = client.load_table_from_dataframe(report, tableRef, job_config=job_config)
job.result()

table = client.get_table(tableRef)
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), tableRef
    )
)

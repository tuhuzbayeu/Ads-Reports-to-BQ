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

os.environ['TZ'] = 'Europe/Moscow'
time.tzset()
today_utc = time.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
print([datetime.today(), today_utc])

#credentials = service_account.Credentials.from_service_account_file('--.json')

project_id = '--'
dataset_id = 'vk-ads'
table_id = 'vk-ads'

dayt = datetime.today().strftime('%m%Y')
table_id = table_id + '_' + dayt

today = datetime.today().strftime('%Y-%m-%d')
fromday = datetime.today().replace(day=1).strftime('%Y-%m-%d')

report = pd.DataFrame(columns=['id', 'campaign_name', 'date', 'clicks', 'spend', 'impressions', 'reach', 'join', 'views', 'video_plays','conversion'])

projects = '--.googlesheet'
projects = requests.get(projects, allow_redirects=True)
projects = projects.content.decode('utf-8')
projects = csv.reader(projects.splitlines(), delimiter=',')
projects = list(projects)

data = '--.googlesheet'
data = requests.get(data, allow_redirects=True)
data = data.content.decode('utf-8')
data = csv.reader(data.splitlines(), delimiter=',')
data = list(data)

logins = pd.DataFrame(columns=['account', 'login', 'token', 'ids'])

for dat in data:
    if dat[0] == 'VK Ads':
        if dat[0] == '': continue
        account = dat[1]
        token = dat[2]

        for row in projects:
            if row[5] == 'VK Ads':
                if row[6] == '': continue
                if row[7] != '': token = row[7]
                logins = logins.append({'account': account, 'login': row[6], 'token': token, 'ids': ''}, ignore_index=True)

logins = logins.drop_duplicates()

for x in logins.index:
    account = logins['account'][x]
    login = logins['login'][x]
    loginToken = logins['token'][x]
    ids = logins['ids'][x]
    campaigns = pd.DataFrame(columns=['id', 'name'])

    print(login)

    try:
        url = 'https://api.vk.com/method/ads.getCampaigns' + '?account_id=' + account + '&client_id=' + login + '&v=5.131' + '&access_token=' + loginToken
        url = requests.get(url)
        url = url.json()

        for i in url['response']:
            campaigns = campaigns.append({'id': str(i['id']), 'name': i['name']}, ignore_index=True)
            if ids == '':
                ids = str(i['id'])
            else:
                ids = ids + ',' + str(i['id'])

        time.sleep(1)

    except Exception as e:
        print(['Ошибка getCampaigns', e])
        continue

    try:
        data = 'https://api.vk.com/method/ads.getStatistics' + '?account_id=' + account + '&ids_type=campaign' + '&ids=' + ids + '&stats_fields=clicks,spent,impressions,reach,join_rate,uniq_views_count,video_plays_unique_100_percents,conversion_count' + '&period=day' + '&date_from=' + fromday + '&date_to=' + today + '&v=5.131' + '&access_token=' + loginToken
        data = requests.get(data, allow_redirects=True)
        data = data.json()

        for j in data['response']:
            id = str(j['id'])
            stats = j['stats']
            name = ''
            for c in campaigns.index:
                if id == campaigns['id'][c]:
                    name = campaigns['name'][c]
                    break
            
            for stat in stats:
                day = stat['day']
                try:
                    spent = stat['spent']
                except:
                    spent = 0
                try:
                    clicks = stat['clicks']
                except:
                    clicks = 0
                try:
                    impressions = stat['impressions']
                except:
                    impressions = 0
                try:
                    reach = stat['reach']
                except:
                    reach = 0
                try:
                    join = stat['join_rate']
                except:
                    join = 0
                try:
                    views = stat['uniq_views_count']
                except:
                    views = 0
                try:
                    video_plays = stat['video_plays_unique_100_percents']
                except:
                    video_plays = 0
                try:
                    conversion = stat['conversion_count']
                except:
                    conversion = 0
                
                report = report.append({'id': login, 'campaign_name': name, 'date': day, 'clicks': clicks, 'spend': spent, 'impressions': impressions, 'reach': reach, 'join': join, 'views': views, 'video_plays': video_plays,'conversion': conversion}, ignore_index=True)
        
        time.sleep(1)

    except Exception as e:
        print(['Ошибка getStatistics', e])
        continue

    time.sleep(1)


report = report.astype(str)
report['clicks'] = report['clicks'].astype(float64)
report['impressions'] = report['impressions'].astype(float64)
report['reach'] = report['reach'].astype(float64)
report['views'] = report['views'].astype(float64)
report['join'] = report['join'].astype(float64)
report['conversion'] = report['conversion'].astype(float64)
report['video_plays'] = report['video_plays'].astype(float64)
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

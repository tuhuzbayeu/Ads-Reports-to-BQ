from google.cloud import bigquery
from google.oauth2 import service_account
from numpy import datetime64, float64
import requests
import pandas as pd
import csv
from datetime import datetime, timedelta
from io import StringIO
import io
import os, time

os.environ['TZ'] = 'Europe/Moscow'
time.tzset()
today_utc = time.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
print([datetime.today(), today_utc])

credentials = service_account.Credentials.from_service_account_file('--.json')

project_id = '--'
dataset_id = 'facebook_ads'
table_id = 'facebook_ads'

day = datetime.today().strftime('%m%Y')
table_id = table_id + '_' + day

date_preset = 'this_month'

report = pd.DataFrame(columns=['id', 'campaign_name', 'date', 'clicks', 'impressions', 'reach', 'views', 'engagement', 'conversions', 'spend'])

projects = '--.googlesheet'
projects = requests.get(projects)
projects = projects.content.decode('utf-8')
projects = csv.reader(projects.splitlines(), delimiter=',')
projects = list(projects)

data = '--.googlesheet'
data = requests.get(data, allow_redirects=True)
data = data.content.decode('utf-8')
data = csv.reader(data.splitlines(), delimiter=',')
data = list(data)

logins = pd.DataFrame(columns=['login', 'token'])

for dat in data:
    if dat[0] == 'Facebook Ads':
        if dat[0] == '': continue

        token = dat[2]
        
        for row in projects:
            if row[5] == 'Facebook Ads':
                if row[6] == '': continue
                if row[7] != '': token = row[7]
                logins = logins.append({'login': row[6], 'token': token}, ignore_index=True)
logins = logins.drop_duplicates()

try:
    for x in logins.index:
        url = 'https://graph.facebook.com/v12.0/' + 'act_' + logins['login'][x] + '/campaigns' + '?fields=name' + '&effective_status=["ACTIVE","PAUSED"]' + '&date_preset=' + date_preset + '&access_token=' + logins['token'][x]
        url = requests.get(url)
        url = url.json()
    
        print(logins['login'][x])

        try:
            for i in url['data']:
                id = i['id']
                name = i['name']

                data = 'https://graph.facebook.com/v12.0/' + id + '/insights' + '?date_preset=' + date_preset + '&fields=actions,spend,inline_link_clicks,impressions,reach,video_30_sec_watched_actions,inline_post_engagement' + '&time_increment=1' + '&access_token=' + logins['token'][x]
                data = requests.get(data)
                data = data.json()

                for j in data['data']:
                    spend = 0
                    try:
                        spend = j['spend']
                    except:
                        spend = 0
                    clicks = 0
                    try:
                        clicks = j['inline_link_clicks']
                    except:
                        clicks = 0
                    impressions = 0
                    try:
                        impressions = j['impressions']
                    except:
                        impressions = 0
                    reach = 0
                    try:
                        reach = j['reach']
                    except:
                        reach = 0
                    date = j['date_start']
                    views = 0
                    try:
                        views = j['video_30_sec_watched_actions'][0]['value']
                    except:
                        views = 0
                    engagement = 0
                    try:
                        engagement = j['inline_post_engagement']
                    except:
                        engagement = 0
                    conversions = 0
                    try:
                        for c in j['actions']:
                            if c['action_type'] == 'lead' or c['action_type'] == 'mobile_app_install':
                                conversions = c['value']
                    except:
                        conversions = 0

                    report = report.append({'id': "act_" + logins['login'][x], 'campaign_name': name, 'date': date, 'clicks': clicks, 'impressions': impressions, 'reach': reach, 'views': views, 'engagement': engagement, 'conversions': conversions, 'spend': spend}, ignore_index=True)

                    time.sleep(1)
        except Exception as e:
            print(['Ошибка insights', e])
            continue

except Exception as e:
    print(['Ошибка campaigns', e])

report = report.astype(str)
report['clicks'] = report['clicks'].astype(float64)
report['impressions'] = report['impressions'].astype(float64)
report['reach'] = report['reach'].astype(float64)
report['views'] = report['views'].astype(float64)
report['engagement'] = report['engagement'].astype(float64)
report['conversions'] = report['conversions'].astype(float64)
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

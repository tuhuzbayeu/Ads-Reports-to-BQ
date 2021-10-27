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
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse  # noqa

os.environ['TZ'] = 'Europe/Moscow'
time.tzset()
today_utc = time.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
print([datetime.today(), today_utc])

credentials = service_account.Credentials.from_service_account_file('--.json')

project_id = '--'
dataset_id = 'tiktok_ads'
table_id = 'tiktok_ads'

dayt = datetime.today().strftime('%m%Y')
table_id = table_id + '_' + dayt

today = datetime.today().strftime('%Y-%m-%d')
fromday = datetime.today().replace(day=1).strftime('%Y-%m-%d')

report = pd.DataFrame(columns=['id', 'campaign_name', 'date', 'clicks', 'spend', 'impressions', 'reach', 'join', 'video_plays','conversion'])
logins = pd.DataFrame(columns=['account', 'login', 'token'])

PATH = "/open_api/v1.2/reports/integrated/get/"

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

for dat in data:
    if dat[0] == 'TikTok Ads':
        if dat[0] == '': continue
        account = dat[1]
        token = dat[2]

        for row in projects:
            if row[5] == 'TikTok Ads':
                if row[6] == '': continue
                if row[7] != '': token = row[7]
                logins = logins.append({'account': account, 'login': row[6], 'token': token}, ignore_index=True)

logins = logins.drop_duplicates()

def build_url(path, query=""):
    scheme, netloc = "https", "business-api.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get(json_str, loginToken):
    args = json.loads(json_str)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": loginToken,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()

for x in logins.index:
    account = logins['account'][x]
    login = logins['login'][x]
    loginToken = logins['token'][x]
    print(login)
    
    if __name__ == '__main__':
        data_level = 'AUCTION_CAMPAIGN'
        end_date = today
        start_date = fromday
        advertiser_id = login
        service_type = 'AUCTION'
        report_type = 'BASIC'
        dimensions_list = ["campaign_id", "stat_time_day"]
        dimensions = json.dumps(dimensions_list)
        page_size = '1000'
        metrics_list = [
            "campaign_name",
            "spend",
            "clicks",
            "impressions",
            "reach",
            "conversion",
            "video_views_p100",
            "follows"
        ]
        metrics = json.dumps(metrics_list)

    my_args = "{\"data_level\": \"%s\", \"end_date\": \"%s\", \"start_date\": \"%s\", \"advertiser_id\": \"%s\", \"service_type\": \"%s\", \"report_type\": \"%s\", \"dimensions\": %s, \"page_size\": \"%s\", \"metrics\": %s}" % (data_level, end_date, start_date, advertiser_id, service_type, report_type, dimensions, page_size, metrics)
    
    data = get(my_args, loginToken)

    if data['message'] == 'OK':
        data = data['data']['list']

        if len(data) > 0:
            for list in data:
                spend = list['metrics']['spend']
                if spend == '0.0': continue

                conversion = list['metrics']['conversion']
                follows = list['metrics']['follows']
                video_views_p100 = list['metrics']['video_views_p100']
                reach = list['metrics']['reach']
                impressions = list['metrics']['impressions']
                campaign_name = list['metrics']['campaign_name']
                clicks = list['metrics']['clicks']
                stat_time_day = list['dimensions']['stat_time_day'][:10]
                
                report = report.append({'id': login, 'campaign_name': campaign_name, 'date': stat_time_day, 'clicks': clicks, 'spend': spend, 'impressions': impressions, 'reach': reach, 'join': follows, 'video_plays': video_views_p100, 'conversion': conversion}, ignore_index=True)

    time.sleep(1)

if len(report.index) > 0:
    report = report.astype(str)
    report['clicks'] = report['clicks'].astype(float64)
    report['impressions'] = report['impressions'].astype(float64)
    report['reach'] = report['reach'].astype(float64)
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

import os
import argparse
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

ROOT = os.path.dirname(__file__)

API_SERVICE_NAME = 'webmasters'
API_VERSION = 'v3'
SCOPE = [
    'https://www.googleapis.com/auth/webmasters.readonly'
]

pd.set_option('max_colwidth', 100)

def auth_service(credentials_path):

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=SCOPE
    )

    service = build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

    return service

def query(service, url, payload):
    response = service.searchanalytics().query(siteUrl=url, body=payload).execute()

    results = []

    for row in response['rows']:
        data = {}

        for i in range(len(payload['dimensions'])):
            data[payload['dimensions'][i]] = row['keys'][i]

        data['clicks'] = row['clicks']
        data['impressions'] = row['impressions']
        data['ctr'] = round(row['ctr'] * 100, 2)
        data['position'] = round(row['position'], 2)

        results.append(data)
    
    return pd.DataFrame.from_dict(results)


if __name__ == '__main__':

    service = auth_service(os.path.join(ROOT, 'credentials.json'))

    parser = argparse.ArgumentParser(
        description='Check website for keyword cannibalization.'
    )

    parser.add_argument(
        '-u',
        '--url',
        type=str,
        required=False,
        default=service.sites().list().execute()['siteEntry'][0]['siteUrl'],
        help='URL address of website to audit'
    )

    parser.add_argument(
        '-s',
        '--startDate',
        type=str,
        required=False,
        default=(date.today() - relativedelta(months=3)).strftime('%Y-%m-%d'),
        help='Start date of the audited period'
    )

    parser.add_argument(
        '-e',
        '--endDate',
        type=str,
        required=False,
        default=date.today().strftime('%Y-%m-%d'),
        help='End date of the audited period'
    )

    parser.add_argument(
        '-q',
        '--query',
        type=str,
        required=False,
        help='Specific query to analyze'
    )

    args = parser.parse_args()

    payload = {
        'startDate': args.startDate,
        'endDate': args.endDate,
        'dimensions': ['query', 'page'],
        'rowLimit': 10000,
        'startRow': 0
    }

    df = query(service, args.url, payload)
    df.to_csv(os.path.join(ROOT, 'all.csv'), index=False)
    print(df.head())

    data = {
        'Total pages': [int(df['page'].nunique())],
        'Total queries': [int(df['query'].nunique())],
        'Total clicks': [int(df['clicks'].sum())],
        'Total impressions': [int(df['impressions'].sum())],
        'Average CTR': [round(df['ctr'].mean(), 2)],
        'Average position': [round(df['position'].mean(), 2)],
        'Average queries per page': [round(int(df['query'].nunique()) / int(df['page'].nunique()), 2)]
    }

    df_stats = pd.DataFrame.from_dict(data)
    print(df_stats.T.head(10))

    df_summary = df.groupby('query').agg(
        unique_pages=('page', 'nunique'),
        total_clicks=('clicks', 'sum'),
        total_impressions=('impressions', 'sum'),
        avg_ctr=('ctr', 'mean'),
        avg_position=('position', 'mean')
    ).sort_values(by='total_clicks', ascending=False)
    print(df_summary.head(10))

    df_cannibalized = df.groupby('query').agg(
        unique_pages=('page', 'nunique'),
        total_clicks=('clicks', 'sum'),
        total_impressions=('impressions', 'sum'),
        avg_ctr=('ctr', 'mean'),
        avg_position=('position', 'mean')
    ).sort_values(by='unique_pages', ascending=False)

    df_cannibalized = df_cannibalized[df_cannibalized['unique_pages'] > 1]
    df_cannibalized.to_csv(os.path.join(ROOT, 'cannibalized.csv'))
    print(df_cannibalized)

    if args.query:
        print(df[df['query']==args.query].sort_values(by='impressions', ascending=False))


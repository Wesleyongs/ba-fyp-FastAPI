import pandas as pd
import requests
import os.path

import decouple
from decouple import config

import boto3
from botocore.exceptions import NoCredentialsError

""" Retrieve API data """
def get_orders(apikey, password, hostname):
    last = 0
    orders = pd.DataFrame()
    while True:
        url = f"https://{apikey}:{password}@{hostname}/admin/api/2021-10/orders.json"
        response = requests.get(url,
                                params={
                                    'status': 'any',
                                    'limit': 250,
                                    'since_id': last,
                                    'fulfillment_status ':'fulfilled'
                                })

        df = pd.DataFrame(response.json()['orders'])
        orders = pd.concat([orders, df])
        last = df['id'].iloc[-1]
        if len(df) < 250:
            break
    return (orders)

""" Generate dataframe for Jamstone channel """

def retrieve_jamstone():
    if os.path.isfile('jamstones.csv'):
        pass
    else:
        apikey = config('JS_KEY')
        password = config('JS_PW')
        hostname = config('JS_HOST')

        jamstone_orders = get_orders(apikey, password, hostname)
        jamstone_orders.sort_values(by='created_at', ascending=False, inplace=True)
        jamstone_orders.to_csv('jamstones.csv')

    jamstones_df = pd.read_csv('jamstones.csv')

    return jamstones_df


""" Generate dataframe for Lavval channel """

def retrieve_lavval():
    if os.path.isfile('lavval.csv'):
        pass
    else:
        apikey = config('LV_KEY')
        password = config('LV_PW')
        hostname = config('LV_HOST')

        lavval_orders = get_orders(apikey, password, hostname)
        lavval_orders.sort_values(by='created_at', ascending=False, inplace=True)
        lavval_orders.to_csv('lavval.csv')

    lavval_df = pd.read_csv('lavval.csv')

    return lavval_df


""" Generate dataframe for Newagefsg channel """

def retrieve_newagefsg():
    if os.path.isfile('newagefsg.csv'):
        pass
    else:
        apikey = config('NA_KEY')
        password = config('NA_PW')
        hostname = config('NA_HOST')

        newagefsg_orders = get_orders(apikey, password, hostname)
        newagefsg_orders.sort_values(by='created_at', ascending=False, inplace=True)
        newagefsg_orders.to_csv('newagefsg.csv')

    newagefsg_df = pd.read_csv('newagefsg.csv')

    return newagefsg_df


def retrieve_all_channels():
    jamstones = retrieve_jamstone()
    lavval = retrieve_lavval()
    newagefsg = retrieve_newagefsg()

    return jamstones, lavval, newagefsg

def upload_to_aws(local_file, bucket, s3_file):
    access_key = config('TEST_KEY')
    secret_key = config('TEST_SECRET')
    s3 = boto3.client('s3', aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def upload_to_aws_change_metadata(local_file, bucket, s3_file):
    session = boto3.Session(
        aws_access_key_id=config('TEST_KEY'),
        aws_secret_access_key=config('TEST_SECRET'),
    )
    s3 = session.resource('s3')
    api_client = s3.meta.client
    response = api_client.copy_object(Bucket=bucket,
                                    Key=s3_file,
                                    ContentType="text/html",
                                    MetadataDirective="REPLACE",
                                    CopySource=bucket + "/" + s3_file)

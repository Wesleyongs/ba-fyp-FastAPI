import ast
import pandas as pd
import numpy as np
import re
import requests
import os.path
import decouple
from decouple import config


def filter_columns(df):
    df = df[['processed_at','total_price','financial_status']]

    return df

# function to change data type and filter
# TODO: Need to filter out the refunds & Voids
def get_change_datatype(df):
    df = df[df.financial_status == "paid"]
    df['total_price'] = df['total_price'].astype(float)
    df['processed_at'] = pd.to_datetime(df['processed_at']).dt.date
    df = df[['processed_at','total_price']]
    df = df.groupby('processed_at').sum().reset_index()
    df["total_price"] = df["total_price"].apply(lambda price: round(price, 2))

    return df

def get_total_revenue(df):
    return round(df['total_price'].sum(),2)

#get data from each channel
def get_jamstone(jamstones_raw):
    data = filter_columns(jamstones_raw)
    result = get_change_datatype(data)
    total_revenue = get_total_revenue(result)
    result.rename(columns = {'processed_at' : 'Date', 'total_price' : 'Jamstones'}, inplace = True)
    x = result["Date"].apply(lambda x: x.strftime("%Y-%m-%d")).to_list()
    y = result["Jamstones"].tolist()

    return [total_revenue,x,y,result]

def get_lavval(lavval_raw):
    data = filter_columns(lavval_raw)
    result = get_change_datatype(data)
    total_revenue = get_total_revenue(result)
    result.rename(columns = {'processed_at' : 'Date', 'total_price' : 'Lavval'}, inplace = True)
    x = result["Date"].apply(lambda x: x.strftime("%Y-%m-%d")).to_list()
    y = result["Lavval"].tolist()

    return [total_revenue,x,y,result]

def get_newage(newagefsg_raw):
    data = filter_columns(newagefsg_raw)
    result = get_change_datatype(data)
    total_revenue = get_total_revenue(result)
    result.rename(columns = {'processed_at' : 'Date', 'total_price' : 'Newagefsg'}, inplace = True)
    x = result["Date"].apply(lambda x: x.strftime("%Y-%m-%d")).to_list()
    y = result["Newagefsg"].tolist()

    return [total_revenue,x,y,result]

#get combined data 
def get_combined_data(jamstones_df, lavval_df, newagefsg_df):
    # jamstones_df = get_jamstone()[3]
    # lavval_df = get_lavval()[3]
    # newagefsg_df = get_newage()[3]
    combined_df = pd.concat([jamstones_df, lavval_df, newagefsg_df],ignore_index=True)
    combined_df = combined_df.fillna(0)
    x = combined_df["Date"].apply(lambda x: x.strftime("%Y-%m-%d")).to_list()
    y1 = combined_df["Jamstones"].tolist()
    y2 = combined_df["Lavval"].tolist()
    y3 = combined_df["Newagefsg"].tolist()

    return [x,y1,y2,y3]

#get all required data in dict
def get_all(jamstones_raw, lavval_raw, newagefsg_raw):
    jamstones_df = get_jamstone(jamstones_raw)
    lavval_df = get_lavval(lavval_raw)
    newagefsg_df = get_newage(newagefsg_raw)

    jamstones_df_part = jamstones_df[3]
    lavval_df_part = lavval_df[3]
    newagefsg_df_part = newagefsg_df[3]

    combined = get_combined_data(jamstones_df_part, lavval_df_part, newagefsg_df_part)
    all = {"JS":jamstones_df ,"LV":lavval_df,"NA":newagefsg_df}

    return all

if __name__ == "__main__":
    pass

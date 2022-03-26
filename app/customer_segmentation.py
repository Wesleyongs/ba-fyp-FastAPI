# %%
from __future__ import division
from datetime import datetime, timedelta
from sklearn.cluster import KMeans
from utils import *

import ast
import pandas as pd
import numpy as np
import re
import requests
import os.path
import decouple
from decouple import config
import copy
import warnings
import seaborn as sns
from operator import attrgetter

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import math
import json

import chart_studio.plotly as py
import plotly.offline as pyoff
import plotly.graph_objs as go
import six


def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)

def source(x):
    if 'shopee' in x.lower():
        return 'Shopee'
    elif 'instagram' in x.lower():
        return 'Instagram'
    else:
        return 'Shopify'
    
def check_format(row):
    if type(row)==str:
        if ((row[0] == '['and row[len(row)-1] == ']' and row[1] == '{'and row[len(row)-2] == '}') or (row[0] == '{'and row[len(row)-1] == '}') )and len(row)>10:
            return True
    else:
        return False

def preprocessing(df):
    df_copy = df.copy()[['id', 'created_at','customer','line_items', 'total_price', 'shipping_address']]

    df_copy['format_check'] = df_copy.apply(lambda x: 'pass' if check_format(x.line_items) else math.nan, axis=1)
    df_copy['format_check'] = df_copy.apply(lambda x: 'pass' if (check_format(x.customer) and x.format_check!=math.nan) else math.nan, axis=1)
    df_copy.dropna(inplace = True)
    
    df_copy['created_at'] = df_copy['created_at'].apply(lambda x: pd.to_datetime(x))

    
    df_copy.line_items = df_copy.line_items.apply(eval).apply(json.dumps)
    df_copy.customer = df_copy.customer.apply(eval).apply(json.dumps)

    df_copy.shipping_address = df_copy.shipping_address.apply(eval).apply(json.dumps)

    # Column names that contain JSON
    json_cols = ['customer', 'line_items','shipping_address']

    def clean_json(x):
        "Create apply function for decoding JSON"
        return json.loads(x)

    # Apply the function column wise to each column of interest
    for x in json_cols:
        df_copy[x] = df_copy[x].apply(clean_json)        
    
    df_copy['customer_id'] = df_copy.apply(lambda x: x.customer['id'], axis=1)
    df_copy['email'] = df_copy.apply(lambda x: x.customer['email'], axis=1)
    df_copy['freq'] = df_copy.groupby('customer_id')['customer_id'].transform('count')
    df_copy['name'] = df_copy.apply(lambda x: x.shipping_address['first_name'], axis=1)
    df_copy["email"].fillna("No Email", inplace = True)
    df_copy["name"].fillna("-", inplace = True)
    
    
    return df_copy


def split_based_on_recency(unique_customer_df, category, shop):
    df_category = unique_customer_df[unique_customer_df["category"] == category]
    df_category_final = df_category[["name", "email", "customer_id", "id", "created_at", "Recency", "freq"]]
    df_category_final.rename(columns={"name": "Name", "email": "Email", "customer_id": "Customer ID", "id": "Last Order ID", "created_at": "Last Order Date", "Recency": "Days Since Last Purchase", "freq": "Purchase Frequency"}, inplace=True)
    df_category_final.to_csv("customer_segmentation_output/" + category + "/" + category + "_customers_" + shop + ".csv", index=False)
    # uploaded_csv = upload_to_aws("customer_segmentation_output/" + category + "/" + category + "_customers_" + shop + ".csv", 'fyp-test-bucket', "customer_segmentation_output/" + category + "/" + category + "_customers_" + shop + '_' + datetime.today().strftime('%Y-%m-%d') + ".csv")
    #remove date time
    # uploaded_csv = upload_to_aws("customer_segmentation_output/" + category + "/" + category + "_customers_" + shop + ".csv", 'fyp-test-bucket', "customer_segmentation_output/" + category + "/" + category + "_customers_" + shop +  ".csv")
    uploaded_csv = upload_to_aws("customer_segmentation_output/" + category + "/" + category + "_customers_" + shop + ".csv", 'ba-fyp-files', "customer_segmentation_output/" + category + "/" + category + "_customers_" + shop +  ".csv")

def get_recency_segmentation(df, shop):
    df['created_at'] = pd.to_datetime(df['created_at'])
    df_user = pd.DataFrame(df['customer_id'].unique())
    df_user.columns = ['customer_id']

    df_max_purchase = df.groupby('customer_id').created_at.max().reset_index()
    df_max_purchase.columns = ['customer_id','MaxPurchaseDate']

    df_max_purchase['Recency'] = (df_max_purchase['MaxPurchaseDate'].max() - df_max_purchase['MaxPurchaseDate']).dt.days

    df_user = pd.merge(df_user, df_max_purchase[['customer_id','Recency']], on='customer_id')
    df = pd.merge(df, df_max_purchase[['customer_id','Recency']], on='customer_id')
    df['category'] = df.apply(lambda x: 'Active' if (x.Recency < 180) else ('Dormant' if (x.Recency < 365) else 'Extinct'), axis=1)	

    maxes = df.groupby(['customer_id']).created_at.transform(max)
    unique_customer_df = df.loc[df.created_at == maxes]

    split_based_on_recency(unique_customer_df, 'Active', shop)
    split_based_on_recency(unique_customer_df, 'Dormant', shop)
    split_based_on_recency(unique_customer_df, 'Extinct', shop)

    return df_user, unique_customer_df

# def perform_clustering(factor):
#     kmeans = KMeans(n_clusters=4)
#     kmeans.fit(df_user[[factor]])
#     df_user[factor + 'Cluster'] = kmeans.predict(df_user[[factor]])
#     df_user = order_cluster(factor + 'cluster', factor,df_user,False)
#     return df_user

def order_cluster(cluster_field_name, target_field_name,df,ascending):
    new_cluster_field_name = 'new_' + cluster_field_name
    df_new = df.groupby(cluster_field_name)[target_field_name].mean().reset_index()
    df_new = df_new.sort_values(by=target_field_name,ascending=ascending).reset_index(drop=True)
    df_new['index'] = df_new.index
    df_final = pd.merge(df,df_new[[cluster_field_name,'index']], on=cluster_field_name)
    df_final = df_final.drop([cluster_field_name],axis=1)
    df_final = df_final.rename(columns={"index":cluster_field_name})
    return df_final

def download_html_graphs(df_user, shop):
    #Revenue vs Frequency
    df_graph = df_user.query("total_price < 50000 and Frequency < 2000")

    plot_data = [
        go.Scatter(
            x=df_graph.query("Segment == 'Low-Value'")['Frequency'],
            y=df_graph.query("Segment == 'Low-Value'")['total_price'],
            mode='markers',
            name='Low',
            marker= dict(size= 7,
                line= dict(width=1),
                color= 'blue',
                opacity= 0.8
               )
        ),
            go.Scatter(
            x=df_graph.query("Segment == 'Mid-Value'")['Frequency'],
            y=df_graph.query("Segment == 'Mid-Value'")['total_price'],
            mode='markers',
            name='Mid',
            marker= dict(size= 9,
                line= dict(width=1),
                color= 'green',
                opacity= 0.5
               )
        ),
            go.Scatter(
            x=df_graph.query("Segment == 'High-Value'")['Frequency'],
            y=df_graph.query("Segment == 'High-Value'")['total_price'],
            mode='markers',
            name='High',
            marker= dict(size= 11,
                line= dict(width=1),
                color= 'red',
                opacity= 0.9
               )
        ),
    ]

    plot_layout = go.Layout(
            yaxis= {'title': "Revenue"},
            xaxis= {'title': "Frequency"},
            title= '[' + shop + '] Customer Segments Based of Revenue and Frequency'
        )
    fig = go.Figure(data=plot_data, layout=plot_layout)
    fig.write_html("customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html", include_plotlyjs="cdn")
    # uploaded_html1 = upload_to_aws("customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + '_' + datetime.today().strftime('%Y-%m-%d') +  ".html")
    # uploaded_html1 = upload_to_aws_change_metadata("customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + '_' + datetime.today().strftime('%Y-%m-%d') +  ".html")
    # uploaded_html1 = upload_to_aws("customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html")
    # uploaded_html1 = upload_to_aws_change_metadata("customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html")
    uploaded_html1 = upload_to_aws("customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html", 'ba-fyp-files', "customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html")
    uploaded_html1 = upload_to_aws_change_metadata("customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html", 'ba-fyp-files', "customer_segmentation_output/graphs/revenue_frequency_clustering_graph_" + shop + ".html")

    #Revenue Recency

    df_graph = df_user.query("total_price < 50000 and Frequency < 2000")

    plot_data = [
        go.Scatter(
            x=df_graph.query("Segment == 'Low-Value'")['Recency'],
            y=df_graph.query("Segment == 'Low-Value'")['total_price'],
            mode='markers',
            name='Low',
            marker= dict(size= 7,
                line= dict(width=1),
                color= 'blue',
                opacity= 0.8
            )
        ),
            go.Scatter(
            x=df_graph.query("Segment == 'Mid-Value'")['Recency'],
            y=df_graph.query("Segment == 'Mid-Value'")['total_price'],
            mode='markers',
            name='Mid',
            marker= dict(size= 9,
                line= dict(width=1),
                color= 'green',
                opacity= 0.5
            )
        ),
            go.Scatter(
            x=df_graph.query("Segment == 'High-Value'")['Recency'],
            y=df_graph.query("Segment == 'High-Value'")['total_price'],
            mode='markers',
            name='High',
            marker= dict(size= 11,
                line= dict(width=1),
                color= 'red',
                opacity= 0.9
            )
        ),
    ]

    plot_layout = go.Layout(
            yaxis= {'title': "Revenue"},
            xaxis= {'title': "Recency"},
            title= '[' + shop + '] Customer Segments Based of Revenue and Recency'
        )
    fig = go.Figure(data=plot_data, layout=plot_layout)
    fig.write_html("customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html", include_plotlyjs="cdn")
    # uploaded_html2 = upload_to_aws("customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + '_' + datetime.today().strftime('%Y-%m-%d') +  ".html")
    # uploaded_html2 = upload_to_aws_change_metadata("customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + '_' + datetime.today().strftime('%Y-%m-%d') +  ".html")
    # uploaded_html2 = upload_to_aws("customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html")
    # uploaded_html2 = upload_to_aws_change_metadata("customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html")
    uploaded_html2 = upload_to_aws("customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html", 'ba-fyp-files', "customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html")
    uploaded_html2 = upload_to_aws_change_metadata("customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html", 'ba-fyp-files', "customer_segmentation_output/graphs/revenue_recency_clustering_graph_" + shop + ".html")

    # Revenue vs Frequency
    df_graph = df_user.query("total_price < 50000 and Frequency < 2000")

    plot_data = [
        go.Scatter(
            x=df_graph.query("Segment == 'Low-Value'")['Recency'],
            y=df_graph.query("Segment == 'Low-Value'")['Frequency'],
            mode='markers',
            name='Low',
            marker= dict(size= 7,
                line= dict(width=1),
                color= 'blue',
                opacity= 0.8
               )
        ),
            go.Scatter(
            x=df_graph.query("Segment == 'Mid-Value'")['Recency'],
            y=df_graph.query("Segment == 'Mid-Value'")['Frequency'],
            mode='markers',
            name='Mid',
            marker= dict(size= 9,
                line= dict(width=1),
                color= 'green',
                opacity= 0.5
               )
        ),
            go.Scatter(
            x=df_graph.query("Segment == 'High-Value'")['Recency'],
            y=df_graph.query("Segment == 'High-Value'")['Frequency'],
            mode='markers',
            name='High',
            marker= dict(size= 11,
                line= dict(width=1),
                color= 'red',
                opacity= 0.9
               )
        ),
    ]

    plot_layout = go.Layout(
            yaxis= {'title': "Frequency"},
            xaxis= {'title': "Recency"},
            title='[' + shop + '] Customer Segments Based of Frequency and Recency'
        )
    fig = go.Figure(data=plot_data, layout=plot_layout)
    fig.write_html("customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html", include_plotlyjs="cdn")
    # uploaded_html1 = upload_to_aws("customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + '_' + datetime.today().strftime('%Y-%m-%d') +  ".html")
    # uploaded_html1 = upload_to_aws_change_metadata("customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + '_' + datetime.today().strftime('%Y-%m-%d') +  ".html")
    # uploaded_html1 = upload_to_aws("customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html")
    # uploaded_html1 = upload_to_aws_change_metadata("customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html", 'fyp-test-bucket', "customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html")
    uploaded_html1 = upload_to_aws("customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html", 'ba-fyp-files', "customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html")
    uploaded_html1 = upload_to_aws_change_metadata("customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html", 'ba-fyp-files', "customer_segmentation_output/graphs/frequency_recency_clustering_graph_" + shop + ".html")

def get_customer_clustering(df_user, unique_customer_df, df, shop):
    kmeans = KMeans(n_clusters=4) #or 3
    kmeans.fit(df_user[['Recency']])
    df_user['RecencyCluster'] = kmeans.predict(df_user[['Recency']])
    df_user = order_cluster('RecencyCluster', 'Recency',df_user,False)

    df_frequency = df.groupby('customer_id').created_at.count().reset_index()
    df_frequency.columns = ['customer_id','Frequency']
    df_user = pd.merge(df_user, df_frequency, on='customer_id')

    kmeans = KMeans(n_clusters=4)
    kmeans.fit(df_user[['Frequency']])
    df_user['FrequencyCluster'] = kmeans.predict(df_user[['Frequency']])

    df_user = order_cluster('FrequencyCluster', 'Frequency',df_user,True)

    df_revenue = df.groupby('customer_id').total_price.sum().reset_index()
    df_user = pd.merge(df_user, df_revenue, on='customer_id')

    kmeans = KMeans(n_clusters=4)
    kmeans.fit(df_user[['total_price']])
    df_user['RevenueCluster'] = kmeans.predict(df_user[['total_price']])
    df_user = order_cluster('RevenueCluster', 'total_price',df_user,True)

    df_user['OverallScore'] = df_user['RecencyCluster'] + df_user['FrequencyCluster'] + df_user['RevenueCluster']
    df_user.groupby('OverallScore')['Recency','Frequency','total_price'].mean()


    df_user['Segment'] = 'Low-Value'
    df_user.loc[df_user['OverallScore']>2,'Segment'] = 'Mid-Value' 
    df_user.loc[df_user['OverallScore']>5,'Segment'] = 'High-Value'

    download_html_graphs(df_user, shop)

    df_clustering = pd.merge(df_user[["customer_id", "Segment", "total_price"]], unique_customer_df, on='customer_id')
    df_clustering = df_clustering.drop(["id", "created_at", "customer", "line_items", "total_price_y", "shipping_address", "format_check", "category"], axis=1)
    df_clustering = df_clustering[["customer_id", "name", "email", "Segment", "Recency", "freq", "total_price_x"]]
    df_clustering.rename(columns={"customer_id": "Customer ID", "name": "Name", "email": "Email", "Segment" : "Customer Current Value", "Recency": "Days Since Last Purchase", "freq": "Purchase Frequency", "total_price_x" : "Total Lifetime Revenue"}, inplace=True)
    df_clustering.to_csv("customer_segmentation_output/clustering/customer_value_" + shop + ".csv", index=False)
    # uploaded_clustering = upload_to_aws("customer_segmentation_output/clustering/customer_value_" + shop + ".csv", 'fyp-test-bucket', "customer_segmentation_output/clustering/customer_value_" + shop + '_' + datetime.today().strftime('%Y-%m-%d') +  ".csv")
    # uploaded_clustering = upload_to_aws("customer_segmentation_output/clustering/customer_value_" + shop + ".csv", 'fyp-test-bucket', "customer_segmentation_output/clustering/customer_value_" + shop + ".csv")
    uploaded_clustering = upload_to_aws("customer_segmentation_output/clustering/customer_value_" + shop + ".csv", 'ba-fyp-files', "customer_segmentation_output/clustering/customer_value_" + shop + ".csv")


def get_all_customer_segmentation(jamstones, lavval, newagefsg):
    lavval_df = preprocessing(lavval)
    jamstones_df = preprocessing(jamstones)
    newagefsg_df = preprocessing(newagefsg)

    total_df = pd.concat([jamstones_df, lavval_df, newagefsg_df], ignore_index=True)

    df_user_and_unique_lavval = get_recency_segmentation(lavval_df, "lavval")
    df_user_and_unique_jamstones = get_recency_segmentation(jamstones_df, "jamstones")
    df_user_and_unique_newagefsg = get_recency_segmentation(newagefsg_df, "newagefsg")
    df_user_and_unique_total = get_recency_segmentation(total_df, "total")

    get_customer_clustering(df_user_and_unique_lavval[0], df_user_and_unique_lavval[1], lavval_df, "Lavval")
    get_customer_clustering(df_user_and_unique_jamstones[0], df_user_and_unique_jamstones[1], jamstones_df, "Jamstones")
    get_customer_clustering(df_user_and_unique_newagefsg[0], df_user_and_unique_newagefsg[1], newagefsg_df, "NewAgeFSG")
    get_customer_clustering(df_user_and_unique_total[0], df_user_and_unique_total[1], total_df, "Total")

    print("Done!")

if __name__ == "__main__":
    get_all_customer_segmentation()
    # pass


# %%

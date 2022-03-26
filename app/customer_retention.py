# %%

import ast
import pandas as pd
import numpy as np
import re
import requests
import os.path
import decouple
from decouple import config
import copy
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
from operator import attrgetter
import matplotlib.colors as mcolors
import math
import json

from datetime import datetime

from utils import *

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
    df_copy['freq'] = df_copy.groupby('customer_id')['customer_id'].transform('count')
    
    
    return df_copy


def get_customer_reorder_rate(df):
    n_orders = df.groupby(['customer_id'])['id'].nunique()
    mult_orders_perc = np.sum(n_orders > 1) / df['customer_id'].nunique()
    mult_orders_perc = round(100 * mult_orders_perc, 2)
    # print(f'{100 * mult_orders_perc:.2f}% of customers ordered more than once.')
    # ax = sns.distplot(n_orders, kde=False, hist=True)
    # ax.set(title='Distribution of number of orders per customer',
    #     xlabel='# of orders', 
    #     ylabel='# of customers');
    return mult_orders_perc

def get_cohort_analysis_pdf_and_png(df, name):
    df = df[['customer_id', 'id', 'created_at']].drop_duplicates()

    df['order_month'] = df['created_at'].dt.to_period('M')
    df['cohort'] = df.groupby('customer_id')['created_at'] \
                    .transform('min') \
                    .dt.to_period('M') 
    df_cohort = df.groupby(['cohort', 'order_month']) \
                .agg(n_customers=('customer_id', 'nunique')) \
                .reset_index(drop=False)
    df_cohort['period_number'] = (df_cohort.order_month - df_cohort.cohort).apply(attrgetter('n'))

    cohort_pivot = df_cohort.pivot_table(index = 'cohort',
                                        columns = 'period_number',
                                        values = 'n_customers')

    cohort_size = cohort_pivot.iloc[:,0]
    retention_matrix = cohort_pivot.divide(cohort_size, axis = 0)                                

    with sns.axes_style("white"):
        fig, ax = plt.subplots(1, 2, figsize=(12, 8), sharey=True, gridspec_kw={'width_ratios': [1, 11]})
        
        # retention matrix
        sns.heatmap(retention_matrix, 
                    mask=retention_matrix.isnull(), 
                    annot=True, 
                    fmt='.0%', 
                    cmap='RdYlGn', #YlGnBu (blue)
                    ax=ax[1])
        ax[1].set_title('[' + name +'] Monthly Cohorts: User Retention', fontsize=16)
        ax[1].set(xlabel='# of periods',
                ylabel='')

        # cohort size
        cohort_size_df = pd.DataFrame(cohort_size).rename(columns={0: 'Cohort Size'})
        white_cmap = mcolors.ListedColormap(['white'])
        sns.heatmap(cohort_size_df, 
                    annot=True, 
                    cbar=False, 
                    fmt='g', 
                    cmap=white_cmap, 
                    ax=ax[0])

        fig.tight_layout()
        plt.savefig('customer_retention_output/' + name +'_cohort_analysis.pdf')
        plt.savefig('customer_retention_output/' + name +'_cohort_analysis.png')
        # uploaded_pdf = upload_to_aws('customer_retention_output/' + name +'_cohort_analysis.pdf', 'fyp-test-bucket', 'customer_retention_output/' + name +'_cohort_analysis_' + datetime.today().strftime('%Y-%m-%d') +'.pdf')
        # uploaded_png = upload_to_aws('customer_retention_output/' + name +'_cohort_analysis.png', 'fyp-test-bucket', 'customer_retention_output/' + name +'_cohort_analysis_' + datetime.today().strftime('%Y-%m-%d') +'.png')
        # uploaded_pdf = upload_to_aws('customer_retention_output/' + name +'_cohort_analysis.pdf', 'fyp-test-bucket', 'customer_retention_output/' + name +'_cohort_analysis' + '.pdf')
        # uploaded_png = upload_to_aws('customer_retention_output/' + name +'_cohort_analysis.png', 'fyp-test-bucket', 'customer_retention_output/' + name +'_cohort_analysis' + '.png')
        uploaded_pdf = upload_to_aws('customer_retention_output/' + name +'_cohort_analysis.pdf', 'ba-fyp-files', 'customer_retention_output/' + name +'_cohort_analysis' + '.pdf')
        uploaded_png = upload_to_aws('customer_retention_output/' + name +'_cohort_analysis.png', 'ba-fyp-files', 'customer_retention_output/' + name +'_cohort_analysis' + '.png')


def get_all_customer_retention(jamstones, lavval, newagefsg):
    lavval_df = preprocessing(lavval)
    jamstones_df = preprocessing(jamstones)
    newagefsg_df = preprocessing(newagefsg)
    total_df = pd.concat([jamstones_df, lavval_df, newagefsg_df], ignore_index=True)

    get_cohort_analysis_pdf_and_png(lavval_df, 'Lavval')
    get_cohort_analysis_pdf_and_png(jamstones_df, 'Jamstones')
    get_cohort_analysis_pdf_and_png(newagefsg_df, 'NewAgeFSG')
    get_cohort_analysis_pdf_and_png(total_df, 'Total')

    lavval_rate = get_customer_reorder_rate(lavval_df)
    jamstones_rate = get_customer_reorder_rate(jamstones_df)
    newagefsg_rate = get_customer_reorder_rate(newagefsg_df)
    total_rate = get_customer_reorder_rate(total_df)

    reorder_rate_dict = {"LV": lavval_rate, "JS": jamstones_rate, "NA": newagefsg_rate, "T": total_rate}
    print(reorder_rate_dict)
    return reorder_rate_dict


    


if __name__ == "__main__":
    get_all_customer_retention()
    # pass


# %%

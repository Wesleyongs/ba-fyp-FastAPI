import ast
import pandas as pd
import numpy as np
import re
import requests
import json
import os.path
import decouple
from decouple import config
import math

from utils import *

def get_all_orders(apikey, password, hostname):
    last = 0
    orders = pd.DataFrame()
    while True:
        url = f"https://{apikey}:{password}@{hostname}/admin/api/2021-10/orders.json"
        response = requests.get(url, params={'status':'any', 'limit':250,'since_id':last})

        df = pd.DataFrame(response.json()['orders'])
        orders = pd.concat([orders, df])
        last = df['id'].iloc[-1]
        if len(df) < 250:
            break
    return (orders)

def retrieve_jamstones():
    if os.path.isfile('jamstones.csv'):
        pass
    else:
        df = get_all_orders(config('JS_KEY'), config('JS_PW'), config('JS_HOST'))
        df.sort_values(by='created_at', ascending=False, inplace=True)
        len(df)
        df.to_csv('jamstones.csv')
    jamstones = pd.read_csv('jamstones.csv')

    return jamstones

def retrieve_lavval():
    if os.path.isfile('lavval.csv'):
        pass
    else:
        df = get_all_orders(config('LV_KEY'), config('LV_PW'), config('LV_HOST'))
        df.sort_values(by='created_at', ascending=False, inplace=True)
        len(df)
        df.to_csv('lavval.csv')
        
    lavval = pd.read_csv('lavval.csv')

    return lavval

def retrieve_newagefsg():
    if os.path.isfile('newagefsg.csv'):
        pass
    else:
        df = get_all_orders(config('NA_KEY'), config('NA_PW'), config('NA_HOST'))
        df.sort_values(by='created_at', ascending=False, inplace=True)
        len(df)
        df.to_csv('newagefsg.csv')
    newagefsg = pd.read_csv('newagefsg.csv')

    return newagefsg

# Preprocessing

#helper functions
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
    df_copy = df.copy()[['id', 'tags', 'current_total_discounts', 'current_total_price','discount_codes','customer', 'discount_applications','line_items', 'total_price', 'shipping_address']]
    df_copy.tags.fillna("Shopify", inplace = True)
    df_copy['format_check'] = df_copy.apply(lambda x: 'pass' if check_format(x.line_items) else math.nan, axis=1)
    df_copy['format_check'] = df_copy.apply(lambda x: 'pass' if (check_format(x.discount_codes) and x.format_check!=math.nan) else math.nan, axis=1)
    df_copy['format_check'] = df_copy.apply(lambda x: 'pass' if (check_format(x.customer) and x.format_check!=math.nan) else math.nan, axis=1)
    df_copy.dropna(inplace = True)
    
    func = np.vectorize(source)
    df_copy['source'] = func(df_copy["tags"])
    df_copy = trim_all_columns(df_copy)
    df_copy['has_discount'] = np.where(df_copy['discount_codes']=='[]', 'Discounted', 'Undiscounted')
    
    df_copy.line_items = df_copy.line_items.apply(eval).apply(json.dumps)
    df_copy.customer = df_copy.customer.apply(eval).apply(json.dumps)
    df_copy.discount_codes = df_copy.discount_codes.apply(eval).apply(json.dumps)
    df_copy.discount_applications = df_copy.discount_applications.apply(eval).apply(json.dumps)
    df_copy.shipping_address = df_copy.shipping_address.apply(eval).apply(json.dumps)

    # Column names that contain JSON
    json_cols = ['customer', 'discount_codes','discount_applications','line_items','shipping_address']

    def clean_json(x):
        "Create apply function for decoding JSON"
        return json.loads(x)

    # Apply the function column wise to each column of interest
    for x in json_cols:
        df_copy[x] = df_copy[x].apply(clean_json)        
    
    df_copy['customer_id'] = df_copy.apply(lambda x: x.customer['id'], axis=1)
    df_copy['freq'] = df_copy.groupby('customer_id')['customer_id'].transform('count')
    
    discount_codes_list = df_copy['discount_codes'].tolist()
    codes_list = [row[0]['code'] if row!=[] else 'None' for row in discount_codes_list ]
    df_copy['codes'] = codes_list

    
    return df_copy


def proportion_discounted_transactions(df):    
    t = df.has_discount.value_counts()
    t_df = pd.DataFrame({'value':t.index, 'freq':t.values})
    x = t_df.value.tolist()
    y = t_df.freq.tolist()
    return [x, y]

def source_orders(df):
    sf = df.source.value_counts()
    temp = pd.DataFrame({'source':sf.index, 'freq':sf.values})
    x = temp.source.tolist()
    y = temp.freq.tolist()
    return [x, y]

def top_discount_codes(df):
    discount_series = df.codes.value_counts()
    top_discount_codes = pd.DataFrame({'discount_code':discount_series.index, 'freq':discount_series.values})
    top_discount_codes = top_discount_codes[1:]
    top10_codes = top_discount_codes[:10]
    x = top10_codes.discount_code.tolist()
    y = top10_codes.freq.tolist()
    return [x, y]

def getUniqueItems(d):
    result = {}
    for key,value in d.items():
        if key not in result.keys():
            result[key] = value
    return result

def proportion_discounts_customers(df):    
    discount = df.drop(df[df.has_discount == False].index)
    no_discount = df.drop(df[df.has_discount == True].index)
    discount['freq'] = discount.groupby('customer_id')['customer_id'].transform('count')
    no_discount['freq'] = no_discount.groupby('customer_id')['customer_id'].transform('count')
    discount_dict = dict(zip(discount.customer_id, discount.freq))
    no_discount_dict = dict(zip(no_discount.customer_id, no_discount.freq))
    new_discount_dict = getUniqueItems(discount_dict)
    new_no_discount_dict = getUniqueItems(no_discount_dict)
    d_keys = list(new_discount_dict.keys())
    n_keys = list(new_no_discount_dict.keys())
    customer_df=pd.DataFrame(columns = ['customer_id', 'discounted_transactions', 'undiscounted_transactions'])
    customer_df['customer_id'] = df['customer_id'].unique()
    customers = customer_df.customer_id.tolist()
    discount = [new_discount_dict[customer] if customer in d_keys else 0 for customer in customers ]
    no_discount = [new_no_discount_dict[customer] if customer in n_keys else 0 for customer in customers ]
    customer_df['discounted_transactions'] = discount
    customer_df['undiscounted_transactions'] = no_discount
    customer_df['freq'] = customer_df['discounted_transactions']+customer_df['undiscounted_transactions']
    top10=customer_df.sort_values(by='freq', ascending=False).head(10)
    
    #customer_id
    x = top10.customer_id.tolist()
    #discounted
    y1 = top10.discounted_transactions.tolist()
    #undiscounted
    y2 = top10.undiscounted_transactions.tolist()
    
    # x, y1, y2
    return [x, y1, y2]

def total_discounts_total_revenue(df):
    total_discounts = df['current_total_discounts'].sum().round(2)
    total_revenue = df['current_total_price'].sum().round(2)
    
    x = ['Total discounts', 'Total revenue']
    y = [total_discounts, total_revenue]
            
    return [x, y]

#wrapper function that calls all the functions above
def all_customer_segmentation_functions(jamstones, lavval, newagefsg):


    jamstones_preprocessed = preprocessing(jamstones)
    lavval_preprocessed = preprocessing(lavval)
    newagefsg_preprocessed = preprocessing(newagefsg)
    aggregate = pd.concat([jamstones_preprocessed, lavval_preprocessed, newagefsg_preprocessed], ignore_index=True)

    proportion_discounted_transactions_jamstones = proportion_discounted_transactions(jamstones_preprocessed)
    proportion_discounted_transactions_lavval = proportion_discounted_transactions(lavval_preprocessed)
    proportion_discounted_transactions_newagefsg = proportion_discounted_transactions(newagefsg_preprocessed)
    proportion_discounted_transactions_aggregate = proportion_discounted_transactions(aggregate)

    source_orders_jamstones = source_orders(jamstones_preprocessed)
    source_orders_lavval = source_orders(lavval_preprocessed)
    source_orders_newagefsg = source_orders(newagefsg_preprocessed)
    source_orders_aggregate = source_orders(aggregate)

    top_discount_codes_jamstones = top_discount_codes(jamstones_preprocessed)
    top_discount_codes_lavval = top_discount_codes(lavval_preprocessed)
    top_discount_codes_newagefsg = top_discount_codes(newagefsg_preprocessed)
    top_discount_codes_aggregate = top_discount_codes(aggregate)

    proportion_discounts_customers_jamstones = proportion_discounts_customers(jamstones_preprocessed)
    proportion_discounts_customers_lavval = proportion_discounts_customers(lavval_preprocessed)
    proportion_discounts_customers_newagefsg = proportion_discounts_customers(newagefsg_preprocessed)
    proportion_discounts_customers_aggregate = proportion_discounts_customers(aggregate)

    total_discounts_total_revenue_jamstones = total_discounts_total_revenue(jamstones_preprocessed)
    total_discounts_total_revenue_lavval = total_discounts_total_revenue(lavval_preprocessed)
    total_discounts_total_revenue_newagefsg = total_discounts_total_revenue(newagefsg_preprocessed)
    total_discounts_total_revenue_aggregate = total_discounts_total_revenue(aggregate)

    jamstones_output = [proportion_discounted_transactions_jamstones, source_orders_jamstones, top_discount_codes_jamstones, proportion_discounts_customers_jamstones, total_discounts_total_revenue_jamstones]
    lavval_output = [proportion_discounted_transactions_lavval, source_orders_lavval, top_discount_codes_lavval, proportion_discounts_customers_lavval, total_discounts_total_revenue_lavval]
    newagefsg_output = [proportion_discounted_transactions_newagefsg, source_orders_newagefsg, top_discount_codes_newagefsg, proportion_discounts_customers_newagefsg, total_discounts_total_revenue_newagefsg]
    aggregate_output = [proportion_discounted_transactions_aggregate, source_orders_aggregate, top_discount_codes_aggregate, proportion_discounts_customers_aggregate, total_discounts_total_revenue_aggregate]

    
    return {'JS': jamstones_output, 
            'LV': lavval_output, 
            'NA': newagefsg_output, 
            'all': aggregate_output}

#proportion of orders with and without discounts
def get_proportion_discounted_transactions(jamstones, lavval, newagefsg):


    jamstones_preprocessed = preprocessing(jamstones)
    lavval_preprocessed = preprocessing(lavval)
    newagefsg_preprocessed = preprocessing(newagefsg)
    aggregate = pd.concat([jamstones_preprocessed, lavval_preprocessed, newagefsg_preprocessed], ignore_index=True)

    proportion_discounted_transactions_jamstones = proportion_discounted_transactions(jamstones_preprocessed)
    proportion_discounted_transactions_lavval = proportion_discounted_transactions(lavval_preprocessed)
    proportion_discounted_transactions_newagefsg = proportion_discounted_transactions(newagefsg_preprocessed)
    proportion_discounted_transactions_aggregate = proportion_discounted_transactions(aggregate)

    return {'Jamstones': proportion_discounted_transactions_jamstones, 
            'Lavval': proportion_discounted_transactions_lavval, 
            'NewAgeFSG': proportion_discounted_transactions_newagefsg, 
            'Aggregate': proportion_discounted_transactions_aggregate}

#source of orders
def get_source_orders(jamstones, lavval, newagefsg):

    jamstones_preprocessed = preprocessing(jamstones)
    lavval_preprocessed = preprocessing(lavval)
    newagefsg_preprocessed = preprocessing(newagefsg)
    aggregate = pd.concat([jamstones_preprocessed, lavval_preprocessed, newagefsg_preprocessed], ignore_index=True)

    source_orders_jamstones = source_orders(jamstones_preprocessed)
    source_orders_lavval = source_orders(lavval_preprocessed)
    source_orders_newagefsg = source_orders(newagefsg_preprocessed)
    source_orders_aggregate = source_orders(aggregate)

    return {'Jamstones': source_orders_jamstones,
            'Lavval': source_orders_lavval,
            'NewAgeFSG': source_orders_newagefsg,
            'Aggregate': source_orders_aggregate}


#top ten discount codes
def get_top_discount_codes(jamstones_df, lavval_df, newagefsg_df):

    jamstones_preprocessed = preprocessing(jamstones_df)
    lavval_preprocessed = preprocessing(lavval_df)
    newagefsg_preprocessed = preprocessing(newagefsg_df)
    aggregate = pd.concat([jamstones_preprocessed, lavval_preprocessed, newagefsg_preprocessed], ignore_index=True)

    top_discount_codes_jamstones = top_discount_codes(jamstones_preprocessed)
    top_discount_codes_lavval = top_discount_codes(lavval_preprocessed)
    top_discount_codes_newagefsg = top_discount_codes(newagefsg_preprocessed)
    top_discount_codes_aggregate = top_discount_codes(aggregate)

    return {'JS': top_discount_codes_jamstones,
            'LV': top_discount_codes_lavval,
            'NA': top_discount_codes_newagefsg,
            'all': top_discount_codes_aggregate}


#proportion of discounted and undiscounted orders of top 10 customers
def get_proportion_discounts_customers(jamstones_df, lavval_df, newagefsg_df):

    jamstones = retrieve_jamstones()
    lavval = retrieve_lavval()
    newagefsg = retrieve_newagefsg()

    jamstones_preprocessed = preprocessing(jamstones)
    lavval_preprocessed = preprocessing(lavval)
    newagefsg_preprocessed = preprocessing(newagefsg)
    aggregate = pd.concat([jamstones_preprocessed, lavval_preprocessed, newagefsg_preprocessed], ignore_index=True)

    proportion_discounts_customers_jamstones = proportion_discounts_customers(jamstones_preprocessed)
    proportion_discounts_customers_lavval = proportion_discounts_customers(lavval_preprocessed)
    proportion_discounts_customers_newagefsg = proportion_discounts_customers(newagefsg_preprocessed)
    proportion_discounts_customers_aggregate = proportion_discounts_customers(aggregate)

    return {'Jamstones': proportion_discounts_customers_jamstones, 
            'Lavval': proportion_discounts_customers_lavval, 
            'NewAgeFSG': proportion_discounts_customers_newagefsg, 
            'Aggregate': proportion_discounts_customers_aggregate}

#proportion of revenue with discounts and without discounts
def get_total_discounts_total_revenue(jamstones, lavval, newagefsg):
    '''
    This function returns the total discounts and total revenue for each of the three stores
    :return: A dictionary with the following keys:
        - Jamstones
        - Lavval
        - NewAgeFSG
        - Aggregate
    '''

    aggregate = pd.concat([jamstones, lavval, newagefsg], ignore_index=True)

    total_discounts_total_revenue_jamstones = total_discounts_total_revenue(jamstones)
    total_discounts_total_revenue_lavval = total_discounts_total_revenue(lavval)
    total_discounts_total_revenue_newagefsg = total_discounts_total_revenue(newagefsg)
    total_discounts_total_revenue_aggregate = total_discounts_total_revenue(aggregate)

    return {'Jamstones': total_discounts_total_revenue_jamstones,
            'Lavval': total_discounts_total_revenue_lavval,
            'NewAgeFSG': total_discounts_total_revenue_newagefsg,
            'Aggregate': total_discounts_total_revenue_aggregate}

if __name__ == "__main__":
    # all_customer_segmentation_functions()
    pass
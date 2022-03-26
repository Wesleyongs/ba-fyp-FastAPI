import ast
import pandas as pd
import numpy as np
import re
import requests
import os.path

# Helper to get data from json
def get_from_json(json_str, item):
    
    # Conver to dict
    try:
        output = ast.literal_eval(json_str)[item]
    except:
        output = ''
        
    # Spending - need to convert to floats
    if item in ['total_spent', 'id']:
        try:
            output = float(output)
        except:
            output = 0
    return output

# Main func to get customer rankings
def get_customer_ranking(df):
    """[To get customer rankings by lifetime spending and order count. Also gives info on shopee/shopify customers]
    Returns:
        [array of arrays]: [customer name, type, lifetime spending, num orders]
    """    
    # Get data
    # df = pd.read_csv('jamstones.csv')
    df_cus = df[['customer','shipping_address','order_number','tags']].copy()

    # Get new columns
    df_cus['customer_id']  = df_cus['customer'].apply(get_from_json, args=('id',))
    df_cus['total_spent']  = df_cus['customer'].apply(get_from_json, args=('total_spent',))
    df_cus['customer_name']  = df_cus['customer'].apply(get_from_json, args=('first_name',))
    df_cus['customer_phone']  = df_cus['shipping_address'].apply(get_from_json, args=('phone',))

    # aggregation
    df_cus_spending = df_cus[[
        'customer_name', 'tags', 'total_spent', 'customer_id','customer_phone',
    ]].groupby('customer_name').agg({
        'tags': lambda x: 'shopee' if 'shopee' in str(x) else 'shopify',
        'total_spent': lambda x: x.max(),
        'customer_phone': 'count',
        'customer_id':  lambda x: x.max(),
    }).reset_index().sort_values(by='total_spent', ascending=False).rename(columns={'customer_phone':'total_orders'})
    
    #output - sending it out as a Json
    return df_cus_spending.to_json(orient="values")

if __name__ == "__main__":
    pass 


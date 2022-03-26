#%%

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
import mlxtend
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori

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
        df = get_all_orders(config('NF_KEY'), config('NF_PW'), config('NF_HOST'))
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
    
def check_format(row):
    if type(row)==str:
        if ((row[0] == '['and row[len(row)-1] == ']' and row[1] == '{'and row[len(row)-2] == '}') or (row[0] == '{'and row[len(row)-1] == '}') )and len(row)>10:
            return True
    else:
        return False

def preprocessing(df):
    df_copy = df.copy()[['id','line_items']]
    df_copy.dropna(inplace = True)
    
    df_copy = trim_all_columns(df_copy)
    df_copy.line_items = df_copy.line_items.apply(eval).apply(json.dumps)

    def clean_json(x):
        "Create apply function for decoding JSON"
        return json.loads(x)

    df_copy['line_items'] = df_copy['line_items'].apply(clean_json)  
    
    return df_copy

#items preprocessing

#name, quantity
#transaction set per row trans_list = [name, name2, name2 ...]
def remove_suffix(input_string, suffix):
    if suffix and input_string.endswith(suffix):
        return input_string[:-len(suffix)]
    return input_string

categories = ['bracelet', 'necklace','pendant', 'earrings', 'ring', 'lamp', 'bottle', 'tower', 'pendulum', 'bead', 'tumble', 'display', 'pouches', 'kits', 'bag charm', 'charm', 'bag', 'kit', 'tree', 'pouch', 'solitaire', 'pointer', 'hedgehog', 'chip']

crystals = ['agate',
    'amazonite',
    'amber',
    'amethyst',
    'ametrine',
    'ammonite',
    'angelite',
    'apache tear',
    'apatite',
    'aquamarine',
    'aragonite',
    'auralite 23',
    'aventurine',
    'azeztulite',
    'azurite',
    'beryl',
    'black rutilated quartz',
    'black spinel',
    'black tourmaline',
    'bloodstone',
    'blue lace agate',
    'boji stone',
    'bronzite',
    'calcite',
    'carborundum',
    'carnelian',
    'celestite',
    'chalcedony',
    'charoite',
    'chiastolite',
    'chrysoberyl',
    'chrysocolla',
    'chrysoprase',
    'citrine',
    'clear quartz',
    'dalmatian jasper',
    'danburite',
    'dendritic',
    'agate',
    'diamond',
    'diopside',
    'emerald',
    'enhydro quartz',
    'fire agate',
    'fire opal',
    'fluorite',
    'fossil coral jasper',
    'freshwater pearl',
    'fuchsite',
    'garnet',
    'gold rutilated quartz',
    'gypsum',
    'hematite',
    'herkimer diamond',
    'hessonite',
    'howlite',
    'hypersthene',
    'imperial topaz',
    'indicolite',
    'iolite',
    'iron nickel meteorite',
    'iron pyrite',
    'jade',
    'jasper',
    'jet',
    'k2',
    'kunzite',
    'kyanite',
    'labradorite',
    'landscape jasper',
    'lapis lazuli',
    'larimar',
    'lemurian seed',
    'lepidolite',
    'libyan gold tektite',
    'malachite',
    'mangano calcite',
    'moldavite',
    'mookaite jasper',
    'moonstone',
    'morganite',
    'moss agate',
    'nebula stone',
    'obsidian',
    'onyx',
    'opal',
    'peridot',
    'petrified wood',
    'phantom quartz',
    'pietersite',
    'prehnite',
    'red jasper',
    'red rutilated quartz',
    'rhodochrosite',
    'rhodonite',
    'rose quartz',
    'ruby',
    'sapphire',
    'selenite',
    'seraphinite',
    'serpentine',
    'shungite',
    'smoky quartz',
    'snowflake obsidian',
    'sodalite',
    'spinel',
    'spirit quartz',
    'staurolite',
    'sugilite',
    'sunstone',
    'tanzanite',
    'tektite',
    'tibetan quartz',
    "tiger's eye",
    'topaz',
    'tourmaline',
    'turquoise',
    'unakite',
    'watermelon tourmaline',
    'zebra jasper',
    'zoisite']

def clean_words(title):
    remove = False
    clean_title = ""
    for char in title:
        if char == "[" or char == "(":
            remove = True
        elif char == "]" or char == ")":
            remove = False
        elif char == "*" and remove == False:
            remove = True
        elif char == "*" and remove == True:
            remove = False

        if remove == False:
            if char == " " and len(clean_title) > 0 and clean_title[-1] != " ":
                clean_title += char
            elif char not in ["]", ")", "*", " "]:
                clean_title += char

    clean_title = [word.capitalize() for word in clean_title.split(" ")]
    return " ".join(clean_title)

def get_category(name):
    for category in categories:
        if category in name.lower():
            return category
    else:
        return 'others'
    
def get_crystal(name):
    for crystal in crystals:
        if crystal in name.lower():
            return crystal
    else:
        return 'others'
    
def remove_live_sale(sublist):
    remove_substring = 'Live Sale $1 Listing'
    new_sublist = []
    for item in sublist:
        if remove_substring not in item:
            new_sublist.append(item)
            
    return new_sublist
    
def items_per_transaction(row):
    num_items = len(row)
    trans_list = []
    categories_list = []
    crystals_list = []
    
    #items
    for i in range(num_items):
        temp_quantity = row[i]['quantity']
        for j in range(temp_quantity):
            substring = '(Not found on Shopify)'
            name = row[i]['name']
            name = clean_words(name).lstrip(" ").rstrip(" ")
            if "Testing" not in name:
                if substring in name:
                    name = remove_suffix(name, substring)
                trans_list.append(name)
    
    trans_list = remove_live_sale(trans_list)
            
    #categories and crystals
    for item in trans_list:
        category = get_category(item)
        crystal = get_crystal(item)
        categories_list.append(category)
        crystals_list.append(crystal)
             
    return trans_list, categories_list, crystals_list


def remove_empty_transactions(row):
    for item in row:
        if item == []:
            row.remove(item)
            
    return row

def remove_others(crystals_per_transaction_list):
    new_crystals_per_transaction_list = []
    for sublist in crystals_per_transaction_list:
        if 'others' not in sublist:
            new_crystals_per_transaction_list.append(sublist)
    return new_crystals_per_transaction_list

def get_crystals_per_transaction(items_df):
    line_items_list = items_df.line_items.tolist()
    crystals_per_transaction_list = [items_per_transaction(row)[2] for row in line_items_list]
    crystals_per_transaction_list = remove_empty_transactions(crystals_per_transaction_list)
    new_crystals_per_transaction_list = remove_others(crystals_per_transaction_list)
    return new_crystals_per_transaction_list
    
def get_crystals_mba(items_df):
    new_crystals_per_transaction_list = get_crystals_per_transaction(items_df)
    te = TransactionEncoder()
    te_ary4 = te.fit(new_crystals_per_transaction_list).transform(new_crystals_per_transaction_list)
    mba_df4 = pd.DataFrame(te_ary4, columns=te.columns_)
    
    #itemsets with at least 1% support
    ap_crystals = mlxtend.frequent_patterns.apriori(mba_df4, min_support = 0.01, use_colnames = True)
    ap_crystals = ap_crystals.sort_values(by=['support'], ascending = False)
    ap_support = list(ap_crystals['support'])[:10]
    ap_itemsets = [list(item_set) for item_set in ap_crystals['itemsets']][:10]
    apriori_list = [ap_support, ap_itemsets]
    
    #association rules
    rules_crystals = mlxtend.frequent_patterns.association_rules(ap_crystals, metric="confidence",min_threshold=0,support_only=False)
    columns = rules_crystals.columns
    rules_list = {}
    for column in columns:
        required = ['antecedents', 'consequents', 'lift']
        if rules_crystals.empty == False:
            if column in required:
                # print(rules_crystals)
                if type(rules_crystals[column][0]) == frozenset:
                    rules_list[column] = [list(sub_item) for sub_item in rules_crystals[column]]
                else:
                    rules_list[column] = list(rules_crystals[column])
            
    return apriori_list, rules_list

def all_crystals_mba(jamstones, lavval, newagefsg):

    jamstones_preprocessed = preprocessing(jamstones)
    lavval_preprocessed = preprocessing(lavval)
    newagefsg_preprocessed = preprocessing(newagefsg)
    aggregate = pd.concat([jamstones_preprocessed, lavval_preprocessed, newagefsg_preprocessed], ignore_index=True)

    crystals_mba_jamstones = get_crystals_mba(jamstones_preprocessed)
    crystals_mba_lavval = get_crystals_mba(lavval_preprocessed)
    crystals_mba_newagefsg = get_crystals_mba(newagefsg_preprocessed)
    crystals_mba_aggregate = get_crystals_mba(aggregate)

    return {'JS': crystals_mba_jamstones,
        'LV': crystals_mba_lavval,
        'NA': crystals_mba_newagefsg,
        'all': crystals_mba_aggregate}

if __name__ == "__main__":
    pass
# %%

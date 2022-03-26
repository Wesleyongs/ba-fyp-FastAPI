import ast
import pandas as pd
import numpy as np
import re
import requests
import os.path
import decouple
from decouple import config
from collections import defaultdict
import mlxtend
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori

from utils import *


#Combine data from 3 channels
def combined_all_transaction(jamstones_df, lavval_df, newagefsg_df):
    # jamstones = retrieve_jamstone()
    # lavval = retrieve_lavval()
    # newagefsg = retrieve_newagefsg()
    
    combined = pd.concat([lavval_df[['line_items']],jamstones_df[['line_items']],newagefsg_df[['line_items']]])
    return combined

#Get all transactions 
def get_transactions(jamstones_df, lavval_df, newagefsg_df):
    combined = combined_all_transaction(jamstones_df, lavval_df, newagefsg_df)
    transaction_list = []
    for index, row in combined.iterrows():
        line_item = []
        if type(row[0]) is not None and type(row[0]) is str:
            data = ast.literal_eval(row[0])
            for purchase in data:
                title = purchase["title"].lower()
                if "testing" not in title and "live sale" not in title:
                    if title not in line_item:
                        line_item.append(title)
        transaction_list.append(line_item)
    return transaction_list

#Put item into categories
def get_category(jamstones_df, lavval_df, newagefsg_df):
    transaction_list = get_transactions(jamstones_df, lavval_df, newagefsg_df)
    categories = ['bracelet', 'necklace','pendant', 'earring', 'ring', 'lamp', 'bottle', 'tower', 'pendulum', 'bead', 'tumble', 'display', 'bag charm', 'charm', 'kit', 'tree', 'pouch', 'solitaire', 'sphere', 'gua sha', 'pixiu']
    cat_list = []
    for each_trans in transaction_list:
        cat_trans = []
        for each_item in each_trans:
            for cat in categories:
                if cat in each_item.lower():
                    cat_trans.append(cat)
        cat_list.append(cat_trans)
    cat_list = [cat for cat in cat_list if len(cat)>1]
    return cat_list

#Calculate mba
def mba(jamstones_df, lavval_df, newagefsg_df):
    cat_list = get_category(jamstones_df, lavval_df, newagefsg_df)
    encode_=mlxtend.preprocessing.TransactionEncoder()
    encode_arr=encode_.fit_transform(cat_list)
    encode_df=pd.DataFrame(encode_arr, columns=encode_.columns_)
    md=mlxtend.frequent_patterns.apriori(encode_df)
    md_minsup=mlxtend.frequent_patterns.apriori(encode_df,min_support=0.02, use_colnames=True)
    rules=mlxtend.frequent_patterns.association_rules(md_minsup, metric="lift",min_threshold=1,support_only=False)
    return rules

#Convert the frozenset column into tuple
def convert_frozenset(jamstones_df, lavval_df, newagefsg_df):
    mba_df = mba(jamstones_df, lavval_df, newagefsg_df)
    cols = ['antecedents','consequents']
    mba_df[cols] = mba_df[cols].applymap(lambda x: tuple(x))
    return mba_df

#Get column as list 
def get_col_list(col_df):
    list = []
    count = 0
    for row in col_df:
        bundle = []
        for each in row:
            bundle.append(each)
        list.append(bundle)
        count+=1
    return list

#Top 10 support
def get_support(jamstones_df, lavval_df, newagefsg_df):
    cat_list = get_category(jamstones_df, lavval_df, newagefsg_df)
    encode_=mlxtend.preprocessing.TransactionEncoder()
    encode_arr=encode_.fit_transform(cat_list)
    encode_df=pd.DataFrame(encode_arr, columns=encode_.columns_)
    md=mlxtend.frequent_patterns.apriori(encode_df)
    md_minsup=mlxtend.frequent_patterns.apriori(encode_df,min_support=0.02, use_colnames=True)
    md_minsup2 = md_minsup.sort_values(by='support',ascending=False).head(10)
    support = md_minsup2['support'].tolist()
    itemsets = md_minsup2['itemsets']
    item = []
    for each in itemsets:
        item.append(list(each))
    support_list = [item] + [support]

    return support_list

#Final output of MBA
def bundle(jamstones_df, lavval_df, newagefsg_df):
    final = []
    support = get_support(jamstones_df, lavval_df, newagefsg_df) #[items] + [support]
    mba_df = convert_frozenset(jamstones_df, lavval_df, newagefsg_df)
    antecedents = get_col_list(mba_df['antecedents'])
    consequents = get_col_list(mba_df['consequents'])
    lift = mba_df['lift'].tolist()
    final = support + [antecedents] + [consequents] + [lift]
    return final

if __name__ == "__main__":
    pass

import ast
import pandas as pd
import numpy as np
import re
import requests
import os.path
import decouple
from decouple import config
from datetime import date
from collections import defaultdict


TODAY_DATE = date.today()
######################################################################################
# HELPER FUNCTIONS
""" Date preprocessing """
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


""" Get filtered data """
def get_filtered_data(df, past_days):
    df['processed_at'] = pd.to_datetime(df['processed_at']).dt.date
    filtered_df = df.loc[(TODAY_DATE - df["processed_at"]).dt.days <= past_days]
    return filtered_df

""" Calculate sales per product """
def get_product_quantity(line_items_df):
    product_quantity_dict = {}
    for index, row in line_items_df.iterrows():
        if type(row[0]) is not None and type(row[0]) is str:
            data = ast.literal_eval(row[0])
            for purchase in data:
                title = purchase["title"]
                new_title = clean_words(title).lstrip(" ").rstrip(" ")
                if "Testing" not in new_title:
                    if new_title not in product_quantity_dict:
                        product_quantity_dict[new_title] = int(purchase["quantity"])
                    else:
                        product_quantity_dict[new_title] += int(purchase["quantity"])
    return product_quantity_dict

def filter_columns(df):
    df = df[['line_items', 'processed_at']]

    return df


""" Step 2 - Generate top sales by quantity """
def get_indiv_sales(line_items_df):
    line_items_df = line_items_df[['line_items']]
    product_quantity_dict = get_product_quantity(line_items_df)

    product_quantity_df = pd.DataFrame.from_dict(product_quantity_dict, orient='index', columns=['quantity'])
    product_quantity_df.sort_values(by='quantity', ascending=False, inplace=True)
    if len(product_quantity_df) < 20:
        top_product_quantity_df = product_quantity_df
    else:
        top_product_quantity_df = product_quantity_df.head(20)
    top_dict = top_product_quantity_df.to_dict()
    top_quantitiy_dict = top_dict["quantity"]

    # Data for bar chart axes
    products = list(top_quantitiy_dict.keys())
    quantities = list(top_quantitiy_dict.values())

    return [products, quantities]

######################################################################################
# Generate sales for lifetime data
""" Step 3a - Generate each channel's sales data & combined data """
def combine_lifetime_indiv_sales(jamstones_raw, lavval_raw, newagefsg_raw):
    """
    Theres 2 parts - individual channels vs combined channels sales

    PART 1:
        For each channel,
        1. retrieve all the data
        2. find the top sales (products and quantity)

    PART 2:
        1. Use each channel's data from part 1
        2. Combine them together
        3. Find the top sales (products and quantity)
    """

    # Part 1
    jamstone_df = filter_columns(jamstones_raw)
    jamstone_sales = get_indiv_sales(jamstone_df)

    lavval_df = filter_columns(lavval_raw)
    lavval_sales = get_indiv_sales(lavval_df)

    newage_df = filter_columns(newagefsg_raw)
    newage_sales = get_indiv_sales(newage_df)

    # Part 2
    combined_df = pd.concat([jamstone_df, lavval_df, newage_df])
    combined_sales = get_indiv_sales(combined_df)

    return jamstone_sales, lavval_sales, newage_sales, combined_sales

""" Step 4a - Generate final dictionary of lifetime sales """
def final_lifetime_sales(jamstones_raw, lavval_raw, newagefsg_raw):
    lifetime_sales = defaultdict(list)
    jamstone_sales, lavval_sales, newage_sales, combined_sales = combine_lifetime_indiv_sales(jamstones_raw, lavval_raw, newagefsg_raw)

    for each in jamstone_sales:
        lifetime_sales["JS"].append(each)
    for each in lavval_sales:
        lifetime_sales["LV"].append(each)
    for each in newage_sales:
        lifetime_sales["NA"].append(each)
    for each in combined_sales:
        lifetime_sales["all"].append(each)

    return dict(lifetime_sales)

######################################################################################
# Generate sales for filtered data
""" Step 3b - Generate each channel's sales data & combined data """
def combine_filtered_indiv_sales(past_days, jamstones_raw, lavval_raw, newagefsg_raw):
    """
    Theres 2 parts - individual channels vs combined channels sales

    PART 1:
        For each channel,
        1. retrieve all the data
        2. filter the data by dates (eg. retrieve past 7 days)
        3. find the top sales (products and quantity)

    PART 2:
        1. Use each channel's filtered data from part 1
        2. Combine them together
        3. Find the top sales (products and quantity)
    """

    # Part 1
    jamstone_df = filter_columns(jamstones_raw)
    filtered_jamstone_df = get_filtered_data(jamstone_df, past_days)
    jamstone_sales = get_indiv_sales(filtered_jamstone_df)

    lavval_df = filter_columns(lavval_raw)
    filtered_lavval_df = get_filtered_data(lavval_df, past_days)
    lavval_sales = get_indiv_sales(filtered_lavval_df)

    newage_df = filter_columns(newagefsg_raw)
    filtered_newage_df = get_filtered_data(newage_df, past_days)
    newage_sales = get_indiv_sales(filtered_newage_df)

    # Part 2
    combined_df = pd.concat([filtered_jamstone_df, filtered_lavval_df, filtered_newage_df])
    combined_sales = get_indiv_sales(combined_df)

    return jamstone_sales, lavval_sales, newage_sales, combined_sales

""" Step 4b - Generated final dictionary of filtered sales """
def final_filtered_sales(jamstones_raw, lavval_raw, newagefsg_raw, num_days_list = [7, 30, 365]):
    final_dict = {}
    
    for num_days in num_days_list:
        filtered_sales = defaultdict(list)
        jamstone_sales, lavval_sales, newage_sales, combined_sales = combine_filtered_indiv_sales(num_days, jamstones_raw, lavval_raw, newagefsg_raw)

        for each in jamstone_sales:
            filtered_sales["JS"].append(each)
        for each in lavval_sales:
            filtered_sales["LV"].append(each)
        for each in newage_sales:
            filtered_sales["NA"].append(each)
        for each in combined_sales:
            filtered_sales["all"].append(each)
        if num_days not in final_dict:
            final_dict[num_days] = dict(filtered_sales)
    return final_dict


if __name__ == "__main__":
    pass

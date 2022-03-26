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


def retrieve_referral_sites_jamstones(jamstones):
    referring_site_jamstones = jamstones['referring_site']
    referring_site_jamstones.fillna(value="NA", inplace=True)

    return referring_site_jamstones


def retrieve_referral_sites_lavval(lavval):
    referring_site_lavval = lavval['referring_site']
    referring_site_lavval.fillna(value="NA", inplace=True)

    return referring_site_lavval


def retrieve_referral_sites_newagefsg(newagefsg):
    referring_site_newagefsg = newagefsg['referring_site']
    referring_site_newagefsg.fillna(value="NA", inplace=True)

    return referring_site_newagefsg


def clean_website(referring_site):
    sites = []
    item_list = ["www", "m", "l", "com", "org", "r", "sg",
                 "lm", "live", "ee", "mail", "captcha", "search", "com/"]
    for index, value in referring_site.items():
        site_words_list = []
        if value == "NA":
            site_words_list.append("NA")
        else:
            website_name = value.split("//")[1]
            website_name_list1 = website_name.split(".")[0:1]
            website_name_list2 = website_name.split(".")[1]
            website_name_list2 = website_name_list2.split("/")[0:1]
            for item in website_name_list1:
                if item in item_list:
                    pass
                else:
                    site_words_list.append(item)
            for item in website_name_list2:
                if item in item_list:
                    pass
                else:
                    site_words_list.append(item)
        if site_words_list == []:
            sites.append("NA")
        else:
            sites.append(site_words_list[0])
    return sites
# store_df is either jamstones, lavval, newagefsg


def prepare_df(store_df, sites_list):
    store_df["referral_site_clean"] = sites_list
    count_list = []
    for item in sites_list:
        count_list.append(1)
    store_df["count"] = count_list
    return store_df


def get_sales_by_channel(store_df):
    sales_by_channel = store_df.groupby("referral_site_clean")["count"].sum()
    sales_by_channel_df = pd.DataFrame(sales_by_channel)
    sales_by_channel_df.sort_values("count", ascending=False, inplace=True)
    sales_by_channel_df.drop("NA", inplace=True)
    return sales_by_channel_df


def get_revenue_by_channel(store_df):
    revenue_by_channel = store_df.groupby("referral_site_clean")[
        "total_price"].sum()
    revenue_by_channel_df = pd.DataFrame(revenue_by_channel)
    revenue_by_channel_df.sort_values(
        "total_price", ascending=False, inplace=True)
    revenue_by_channel_df.drop("NA", inplace=True)
    return revenue_by_channel_df


def get_final_merged_referral_site_list(revenue_by_channel_df, sales_by_channel_df):
    combined_sites_df = pd.merge(
        revenue_by_channel_df, sales_by_channel_df, on="referral_site_clean")
    row_list = []

    for index, rows in combined_sites_df.iterrows():
        my_list = [index, rows["total_price"], rows["count"]]
        row_list.append(my_list)

    for row in row_list:
        row[0] = row[0].capitalize()
        row[1] = row[1].round(2)
        row[2] = int(row[2])

    return row_list


def combine_channels(jamstones, lavval, newagefsg):
    combine_channels_list = copy.deepcopy(jamstones)
    channels_in_list = []
    for channel in combine_channels_list:
        channels_in_list.append(channel[0])
    for row in lavval:
        if row[0] not in channels_in_list:
            combine_channels_list.append(row)
        for channel in combine_channels_list:
            if (row[0] == channel[0]):
                channel[1] += row[1]
                channel[2] += row[2]

    channels_in_list = []
    for channel in combine_channels_list:
        channels_in_list.append(channel[0])
    for row in newagefsg:
        if row[0] not in channels_in_list:
            combine_channels_list.append(row)
        for channel in combine_channels_list:
            if (row[0] == channel[0]):
                channel[1] += row[1]
                channel[2] += row[2]
    return combine_channels_list


def get_jamstones_final(jamstones):
    # jamstones = retrieve_jamstones()
    referring_site_jamstones = retrieve_referral_sites_jamstones(jamstones)
    clean_jamstones = clean_website(referring_site_jamstones)
    prepared_jamstones = prepare_df(jamstones, clean_jamstones)
    sales_jamstones = get_sales_by_channel(prepared_jamstones)
    revenue_jamstones = get_revenue_by_channel(prepared_jamstones)
    merged_jamstones = get_final_merged_referral_site_list(
        revenue_jamstones, sales_jamstones)

    return merged_jamstones


def get_lavval_final(lavval):
    # lavval = retrieve_lavval()
    referring_site_lavval = retrieve_referral_sites_lavval(lavval)
    clean_lavval = clean_website(referring_site_lavval)
    prepared_lavval = prepare_df(lavval, clean_lavval)
    sales_lavval = get_sales_by_channel(prepared_lavval)
    revenue_lavval = get_revenue_by_channel(prepared_lavval)
    merged_lavval = get_final_merged_referral_site_list(
        revenue_lavval, sales_lavval)

    return merged_lavval


def get_newagefsg_final(newagefsg):
    # newagefsg = retrieve_newagefsg()
    referring_site_newagefsg = retrieve_referral_sites_newagefsg(newagefsg)
    clean_newagefsg = clean_website(referring_site_newagefsg)
    prepared_newagefsg = prepare_df(newagefsg, clean_newagefsg)
    sales_newagefsg = get_sales_by_channel(prepared_newagefsg)
    revenue_newagefsg = get_revenue_by_channel(prepared_newagefsg)
    merged_newagefsg = get_final_merged_referral_site_list(
        revenue_newagefsg, sales_newagefsg)

    # print(merged_newagefsg)
    return merged_newagefsg

# Main wrapper function
def get_final_combined_referral_dict(jamstones, lavval, newagefsg):
    """[summary]

    Returns:
        {"JS": [List of Jamstones referral sites], "LV": [ List of Lavval ...], "NA": [List of Newagefsg], "all": [Combined list]}

        [list 1]: [ [x], [y]],
        [list 2]: [ [x], [y]],        
    """
    merged_jamstones = get_jamstones_final(jamstones)
    merged_lavval = get_lavval_final(lavval)
    merged_newagefsg = get_newagefsg_final(newagefsg)

    final_dict = {"JS": merged_jamstones, "LV": merged_lavval, "NA": merged_newagefsg}
    final_combined_referral_list = combine_channels(merged_jamstones, merged_lavval, merged_newagefsg)
    final_combined_referral_df = pd.DataFrame(final_combined_referral_list)
    final_combined_referral_df.columns = ["index", "total_price", "sales"]
    final_combined_referral_df.sort_values(
        "total_price", ascending=False, inplace=True)

    final_combined_referral_list = []
    for index, rows in final_combined_referral_df.iterrows():
        my_list = [rows["index"], rows["total_price"], rows["sales"]]
        final_combined_referral_list.append(my_list)

    for row in final_combined_referral_list:
        row[1] = round(row[1], 2)

    final_dict["all"] = final_combined_referral_list
    return final_dict


if __name__ == "__main__":
    pass
    # get_final_combined_referral_dict()
    # get_jamstones_final()

# %%

import pandas as pd
import requests
import datetime as dt

from decouple import config

""" Retrieve orders """


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
                                    'fulfillment_status ': 'fulfilled'
                                })

        df = pd.DataFrame(response.json()['orders'])
        orders = pd.concat([orders, df])
        # This try statement is for the case where orders / 250 == 0
        try:
            last = df['id'].iloc[-1]
        except:
            break
        if len(df) < 250:
            break
    return (orders)


""" Update all 3 shop CSV """


def update_files():

    shop_names = ['jamstones', 'lavval', 'newagefsg']

    for index, shop in enumerate(['JS', 'LV', 'NA']):

        print(f"Start updating csv for {shop}")
        begin_time = dt.datetime.now()
        apikey = config(f'{shop}_KEY')
        password = config(f'{shop}_PW')
        hostname = config(f'{shop}_HOST')
        df = get_orders(apikey, password, hostname)
        df.sort_values(by='created_at', ascending=False, inplace=True)
        df.to_csv(f'{shop_names[index]}.csv')
        print(f"Done with {shop} - Time taken: ",
              dt.datetime.now() - begin_time)

    print("Done updating csv")
    return("Success")


if __name__ == "__main__":
    update_files()

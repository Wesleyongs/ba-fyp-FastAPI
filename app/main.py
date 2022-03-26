"""
Run the microservice using : uvicorn main:app --reload
or 
npm run serve to start the front end hosting as well 
"""

import datetime as dt
import json
import os
import sys
from typing import List

import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from customer import *
from customer_retention import *
from customer_segmentation import *
from db import *
from discount_codes import *
from referral_sites import *
from revenue import *
from top_products import *
from crystals_mba import *
from category_mba import *
from utils import *

sys.path.append('postgres/')
import crud
import models
import schemas
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


###################################################################################################

""" Refresh Data """


@app.get("/refreshData")
def home():
    begin_time = dt.datetime.now()
    res = update_files()
    print("Time taken", dt.datetime.now() - begin_time)
    # sending out as a list
    return res


###################################################################################################

""" Update postgres database for all analyses """
@app.get("/update-all-analyses")
def update_postgres_all(db: Session = Depends(get_db)):
    begin_time = dt.datetime.now()

    jamstones, lavval, newagefsg = retrieve_all_channels()

    """ Revenue """
    revenue_data = get_all(jamstones, lavval, newagefsg)
    for shop, v in revenue_data.items():
        for date, rev in zip(v[1], v[2]):
            crud.create_revenue(
                db=db,
                revenue=models.Revenue(date=date, shop=shop, revenue=int(rev)))

    """ Customers """
    customer_data = json.loads(get_customer_ranking(jamstones))
    customer_shop = "JS"
    customer_counter = 1

    for i in customer_data:
        name = i[0]
        channel = i[1]
        total = int(i[2])
        count = int(i[3])
        id = int(i[4])

        crud.create_customers(
            db=db,
            customers=models.Customers(name=name, channel=channel, total=total, count=count, shop=customer_shop, id=id))
        print(f"Done with {customer_shop} - {name} - {customer_counter}/{len(customer_data)}")
        customer_counter += 1

    """ Referral Sites """
    referral_data = get_final_combined_referral_dict(jamstones, lavval, newagefsg)

    for k, value in referral_data.items():
        counter = 1
        shop = k
        for v in value:
            channel = v[0]
            total = v[1]
            count = v[2]

            crud.create_referrals(
                db=db,
                referrals=models.Referrals(shop=shop, channel=channel, total=total, count=count))
            counter += 1

    """ Discount Code """
    discount_data = get_top_discount_codes(jamstones, lavval, newagefsg)
    for shop, v in discount_data.items():
        for name, count in zip(v[0], v[1]):
            crud.create_discounts(
                db=db,
                discounts=models.Discounts(
                    name=name,
                    shop=shop,
                    count=count))

    """ Lifetime Products """

    crud.remove_lifetime_products(db)
    lifetime_data = final_lifetime_sales(jamstones, lavval, newagefsg)
    for shop, v in lifetime_data.items():
        for product, quantity in zip(v[0], v[1]):
            crud.create_lifetime_product(
                db=db,
                lifetimeProduct=models.LifetimeProducts(
                    shop=shop,
                    product=product,
                    quantity=quantity))

    """ Filtered Products """

    crud.remove_filtered_products(db)
    filtered_data = final_filtered_sales(jamstones, lavval, newagefsg)
    for past_days, all_items in filtered_data.items():
        for shop, v in all_items.items():
            if v[0] != [] and v[1] != []:
                for product, quantity in zip(v[0], v[1]):
                    crud.create_filtered_product(
                        db=db,
                        filteredProduct=models.FilteredProducts(
                            shop=shop,
                            product=product,
                            quantity=quantity,
                            past_days=past_days))

    """ Source Orders """
    crud.remove_source_orders(db)
    source_orders = get_source_orders(jamstones, lavval, newagefsg)
    for shop, details in source_orders.items():
        for channel, quantity in zip(details[0], details[1]):
            crud.create_source_order(
                db=db,
                sourceOrder=models.SourceOrders(
                    shop=shop,
                    channel=channel,
                    quantity=quantity))

    """ Discounts Revenue """
    crud.remove_discounts_revenue(db)
    discounts_revenue = get_total_discounts_total_revenue(jamstones, lavval, newagefsg)
    for shop, details in discounts_revenue.items():
        for category, amount in zip(details[0], details[1]):
            crud.create_discounts_revenue(
                db=db,
                discountsRevenue=models.DiscountsRevenue(
                    shop=shop,
                    category=category,
                    amount=amount))
    
    """ MBA Crystal """
    crud.delete_mba_crystal(db, models.MBAcrystal1)
    crud.delete_mba_crystal(db, models.MBACrystal2)

    crystal_data = all_crystals_mba(jamstones, lavval, newagefsg)

    for shop in ['LV', 'NA', 'all', 'JS']:

        # crystal 1
        supports = crystal_data[shop][0][0]
        crystals = crystal_data[shop][0][1]
        for idx, support in enumerate(supports):
            crud.create_mba_crystal1(db, support, crystals[idx][0], shop)

        # crystal 2
        c2 = crystal_data[shop][1]
        if c2 == {}:continue
        for a, c, l in zip(c2['antecedents'], c2['consequents'], c2['lift']):
            a = [i for i in a][0]
            c = [i for i in c][0]
            crud.create_mba_crystal2(db, a, c, l, shop)

    """ MBA Category """
    crud.remove_mba_category1(db)
    crud.remove_mba_category2(db)
    mba_categories = bundle(jamstones, lavval, newagefsg)
    for cat1, cat2 in zip(mba_categories[0], mba_categories[1]):
        crud.create_mba_category1(
            db,
            MBACategory1=models.MBACategory1(
                category=cat1,
                support_percentage=cat2
            ))

    for cat1, cat2, cat3 in zip(mba_categories[2], mba_categories[3], mba_categories[4]):
        crud.create_mba_category2(
            db,
            MBACategory2=models.MBACategory2(
                antecedents=cat1,
                consequents=cat2,
                lift=cat3
            ))

    """ Update customer retention """
    get_all_customer_retention(jamstones, lavval, newagefsg)
    
    """ Update customer segmentation """
    all_customer_segmentation_functions(jamstones, lavval, newagefsg)

    return "Updated all analyses", (dt.datetime.now() - begin_time)

###################################################################################################
""" Revenue PG """

# Update postgres revenue - everything


@app.get("/postgres/update-db/revenue")
def update_postgres_revenue(db: Session = Depends(get_db)):
    jamstones, lavval, newagefsg = retrieve_all_channels()
    revenue_data = get_all(jamstones, lavval, newagefsg)
    for shop, v in revenue_data.items():
        for date, rev in zip(v[1], v[2]):
            crud.create_revenue(
                db=db,
                revenue=models.Revenue(date=date, shop=shop, revenue=int(rev)))
            print(f"Done with {shop} - {date} - {rev}")
    return revenue_data

# Create postgres revenue


@app.post("/postgres/create/revenue", response_model=schemas.Revenue)
def create_revenue(revenue: schemas.RevenueCreate, db: Session = Depends(get_db)):
    # db_user = crud.get_user_by_email(db, email=user.email)
    # if db_user:
    # raise HTTPException(status_code=400, detail="Email already registered")
    return crud.create_revenue(db=db, revenue=revenue)

# Get rev of a single shop , response_model=List[schemas.Revenue]


@app.get("/postgres/read/revenue/{shop}")
def get_revenue(shop: str, db: Session = Depends(get_db)):
    db_revenue = crud.get_revenue(db, shop=shop)
    if db_revenue is None:
        raise HTTPException(
            status_code=404, detail=f"{shop} not found, shopname is case sensitive")
    res = [[], [], []]
    for i in db_revenue:
        res[0].append(i.shop)
        res[1].append(i.date)
        res[2].append(i.revenue)
    return res


###################################################################################################

""" Top Customers PG """

# Update postgres customers - everything


@app.get("/postgres/update-db/customers")
def update_postgres_customers(db: Session = Depends(get_db)):
    #TODO: Update main function to retrieve df
    jamstones, lavval, newagefsg = retrieve_all_channels()
    data = json.loads(get_customer_ranking(jamstones))
    shop = "JS"
    counter = 1

    for i in data:
        name = i[0]
        channel = i[1]
        total = int(i[2])
        count = int(i[3])
        id = int(i[4])

        crud.create_customers(
            db=db,
            customers=models.Customers(name=name, channel=channel, total=total, count=count, shop=shop, id=id))
        print(f"Done with {shop} - {name} - {counter}/{len(data)}")
        counter += 1

    return data


# Retrieve customers


@app.get("/postgres/read/customers/{shop}")
def get_customers(shop: str, db: Session = Depends(get_db)):
    db_customers = crud.get_customers(db, shop=shop)
    if db_customers is None:
        raise HTTPException(
            status_code=404, detail=f"{shop} not found, shopname is case sensitive")
    # format data to fit front ends
    payload = []
    for i in db_customers:
        payload.append(
            [i.name, i.channel, i.total, i.count, i.id])
    return payload

###################################################################################################


""" Referrals Sites PG """


# Update postgres refeeral sites


@app.get("/postgres/update-db/referrals")
def create_referrals(db: Session = Depends(get_db)):
    jamstones, lavval, newagefsg = retrieve_all_channels()
    data = get_final_combined_referral_dict(jamstones, lavval, newagefsg)

    for k, value in data.items():
        counter = 1
        shop = k
        for v in value:
            channel = v[0]
            total = v[1]
            count = v[2]

            crud.create_referrals(
                db=db,
                referrals=models.Referrals(shop=shop, channel=channel, total=total, count=count))
            print(f"Done with {shop} - {channel} - {counter}/{len(value)}")
            counter += 1

    return data


# Retrieve referral sites


@app.get("/postgres/read/referrals")
def get_referrals(db: Session = Depends(get_db)):
    db_referrals = crud.get_referrals(db)
    if db_referrals is None:
        raise HTTPException(
            status_code=404, detail=f"{shop} not found, shopname is case sensitive")
    payload = {"JS": [], "LV": [], "NA": [], 'all': []}
    for i in db_referrals:
        payload[i.shop].append([i.channel, i.total, i.count])
    return payload


###################################################################################################
""" Top 10 Discounts PG """

# Update postgres discount codes - everything


@app.get("/postgres/update-db/discounts")
def update_postgres_discounts(db: Session = Depends(get_db)):
    jamstones, lavval, newagefsg = retrieve_all_channels()
    data = get_top_discount_codes(jamstones, lavval, newagefsg)
    for shop, v in data.items():
        for name, count in zip(v[0], v[1]):
            crud.create_discounts(
                db=db,
                discounts=models.Discounts(
                    name=name,
                    shop=shop,
                    count=count))
            print(f"Done with {shop} - {name} - {count}")
    return "Done"


# Get all disc from shop


@app.get("/postgres/read/discounts")
def get_discounts(db: Session = Depends(get_db)):
    db_discounts = crud.get_discounts(db)
    if db_discounts is None:
        raise HTTPException(
            status_code=404, detail=f"not found, shopname is case sensitive")
    payload = {"JS": [], "NA": [], "LV": [], "all": []}
    for i in db_discounts:
        payload[i.shop].append([i.name, i.count])
    return payload


###################################################################################################

""" Top Products - Lifetime & Filtered """

# Update postgres lifetime products


@app.get("/postgres/update-db/lifetime-products")
def update_top_lifetime_products(db: Session = Depends(get_db)):
    crud.remove_lifetime_products(db)
    jamstones, lavval, newagefsg = retrieve_all_channels()
    data = final_lifetime_sales(jamstones, lavval, newagefsg)
    for shop, v in data.items():
        for product, quantity in zip(v[0], v[1]):
            crud.create_lifetime_product(
                db=db,
                lifetimeProduct=models.LifetimeProducts(
                    shop=shop,
                    product=product,
                    quantity=quantity))
            print(f"Done with {shop} - {product} - {quantity}")
    return data

# Retrieve lifetime products by shop


@app.get("/postgres/read/lifetime-products/{shop}", response_model=List[schemas.LifetimeProducts])
def get_top_lifetime_products(shop: str, db: Session = Depends(get_db)):
    db_products = crud.get_lifetime_products(db, shop=shop)
    if db_products is None:
        raise HTTPException(
            status_code=404, detail=f"Top products for {shop} not found")
    return db_products

# Update postgres filtered products


@app.get("/postgres/update-db/filtered-products")
def update_top_filtered_products(db: Session = Depends(get_db)):
    crud.remove_filtered_products(db)
    jamstones, lavval, newagefsg = retrieve_all_channels()
    data = final_filtered_sales(jamstones, lavval, newagefsg)
    for past_days, all_items in data.items():
        for shop, v in all_items.items():
            if v[0] != [] and v[1] != []:
                for product, quantity in zip(v[0], v[1]):
                    crud.create_filtered_product(
                        db=db,
                        filteredProduct=models.FilteredProducts(
                            shop=shop,
                            product=product,
                            quantity=quantity,
                            past_days=past_days))
                    print(
                        f"Done with {shop} - {product} - {quantity} - {past_days}")
    return data

# Retrieve filtered products by shop and date


@app.get("/postgres/read/filtered-products/{shop}/{past_days}", response_model=List[schemas.FilteredProducts])
def get_top_filtered_products(shop: str, past_days: int, db: Session = Depends(get_db)):
    db_products = crud.get_filtered_products(
        db, shop=shop, past_days=past_days)
    if db_products is None or len(db_products) == 0:
        raise HTTPException(
            status_code=404, detail=f"Top products for {shop} not found")
    return db_products


###################################################################################################

""" Source of orders """

# Update postgres source orders


@app.get("/postgres/update-db/source-orders")
def update_source_orders(db: Session = Depends(get_db)):
    crud.remove_source_orders(db)
    jamstones, lavval, newagefsg = retrieve_all_channels()
    source_orders = get_source_orders(jamstones, lavval, newagefsg)
    for shop, details in source_orders.items():
        for channel, quantity in zip(details[0], details[1]):
            crud.create_source_order(
                db=db,
                sourceOrder=models.SourceOrders(
                    shop=shop,
                    channel=channel,
                    quantity=quantity))
            print(f"Done with {shop} - {channel} - {quantity}")

    return source_orders

# Retrieve source orders by shop


@app.get("/postgres/read/source-orders/{shop}")
def retrieve_source_orders(shop: str, db: Session = Depends(get_db)):
    db_orders = crud.obtain_source_orders(db, shop=shop)
    if db_orders is None:
        raise HTTPException(
            status_code=404, detail=f"Source orders for {shop} not found")

    final_orders = crud.format_source_orders(db_orders, shop)
    return final_orders

###################################################################################################


""" Proportion of revenue with discounts and without discounts """

# Update postgres revenue with and without discounts


@app.get("/postgres/update-db/discounts-revenue")
def update_discounts_revenue(db: Session = Depends(get_db)):
    crud.remove_discounts_revenue(db)
    jamstones, lavval, newagefsg = retrieve_all_channels()
    discounts_revenue = get_total_discounts_total_revenue(jamstones, lavval, newagefsg)
    for shop, details in discounts_revenue.items():
        for category, amount in zip(details[0], details[1]):
            crud.create_discounts_revenue(
                db=db,
                discountsRevenue=models.DiscountsRevenue(
                    shop=shop,
                    category=category,
                    amount=amount))
            print(f"Done with {shop} - {category} - {amount}")
    return discounts_revenue


# Get revenue with and without discounts


@app.get("/postgres/read/discounts-revenue/{shop}")
def get_discounts_revenue(shop: str, db: Session = Depends(get_db)):
    db_discounts_revenue = crud.obtain_discounts_revenue(db, shop=shop)
    if db_discounts_revenue is None:
        raise HTTPException(
            status_code=404, detail=f"Revenues (with & without discounts) for {shop} not found")
    final_discounts = crud.format_discounts_revenue(db_discounts_revenue, shop)
    return final_discounts


###################################################################################################

""" Market Basket Analysis (Crystal) """


@app.get("/postgres/read/mba-crystal1")
def read_mba_crystal1(db: Session = Depends(get_db)):
    payload = {}
    for shop in ['JS', 'LV', 'NA', 'all']: 
        payload[shop]=crud.get_mba_crystal(db, models.MBAcrystal1, shop)
    return payload


@app.get("/postgres/read/mba-crystal2")
def read_mba_crystal2(db: Session = Depends(get_db)):
    payload = {}
    for shop in ['JS', 'LV', 'NA', 'all']: 
        payload[shop]=crud.get_mba_crystal(db, models.MBACrystal2, shop)
    return payload

@app.get("/postgres/update-db/delete-mba-crystal")
def delete_mba_crystal(db: Session = Depends(get_db)):

    crud.delete_mba_crystal(db, models.MBAcrystal1)
    crud.delete_mba_crystal(db, models.MBACrystal2)

@app.get("/postgres/update-db/mba-crystal")
def update_mba_crystal(db: Session = Depends(get_db)):

    crud.delete_mba_crystal(db, models.MBAcrystal1)
    crud.delete_mba_crystal(db, models.MBACrystal2)

    jamstones, lavval, newagefsg = retrieve_all_channels()
    data = all_crystals_mba(jamstones, lavval, newagefsg)

    for shop in ['LV', 'NA', 'all', 'JS']:

        # crystal 1
        supports = data[shop][0][0]
        crystals = data[shop][0][1]
        for idx, support in enumerate(supports):
            crud.create_mba_crystal1(db, support, crystals[idx][0], shop)

        # crystal 2
        c2 = data[shop][1]
        if c2 == {}:continue
        for a, c, l in zip(c2['antecedents'], c2['consequents'], c2['lift']):
            a = [i for i in a][0]
            c = [i for i in c][0]
            crud.create_mba_crystal2(db, a, c, l, shop)

    return "Done"


###################################################################################################

""" Market Basket Analysis (Category) """

# Update postgres MBA categories 1 and 2


@app.get("/postgres/update-db/mba-category")
def update_mba_category(db: Session = Depends(get_db)):
    crud.remove_mba_category1(db)
    crud.remove_mba_category2(db)
    jamstones, lavval, newagefsg = retrieve_all_channels()
    mba_categories = bundle(jamstones, lavval, newagefsg)
    for cat1, cat2 in zip(mba_categories[0], mba_categories[1]):
        crud.create_mba_category1(
            db,
            MBACategory1=models.MBACategory1(
                category=cat1,
                support_percentage=cat2
            ))
        print(f"Done with {cat1} - {cat2}")

    for cat1, cat2, cat3 in zip(mba_categories[2], mba_categories[3], mba_categories[4]):
        crud.create_mba_category2(
            db,
            MBACategory2=models.MBACategory2(
                antecedents=cat1,
                consequents=cat2,
                lift=cat3
            ))
        print(f"Done with {cat1} - {cat2} - {cat3}")

    return mba_categories


# Retrieve mba categories 1


@app.get('/postgres/read/mba-category1')
def get_mba_category1(db: Session = Depends(get_db)):
    categories = crud.obtain_mba_category1(db)
    if categories is None:
        raise HTTPException(
            status_code=404, detail=f"Bundle details cannot be found")
    final_categories = crud.format_mba_category1(categories)
    return final_categories


# Retrieve mba categories 2


@app.get('/postgres/read/mba-category2')
def get_mba_category2(db: Session = Depends(get_db)):
    categories = crud.obtain_mba_category2(db)
    if categories is None:
        raise HTTPException(
            status_code=404, detail=f"Bundle details cannot be found")
    final_categories = crud.format_mba_category2(categories)
    return final_categories


##################################
##################################
""" U N U S E D    E N D P O I N T S """
##################################
##################################


""" Top Discount Codes (Unused) """


@app.get("/discount-codes")
def get_customer_segmentation():
    begin_time = dt.datetime.now()
    print("Start get discount codes")
    result = all_customer_segmentation_functions()
    print("Done with all discount codes")
    print("Time taken", dt.datetime.now() - begin_time)
    return result


""" Proportion of discounted and undiscounted orders of top 10 customers """


@app.get("/proportion-discounts-customers")
def proportion_discounts_customers():
    begin_time = dt.datetime.now()
    print("Start getting Proportion of discounted and undiscounted orders of top 10 customers ")
    result = get_proportion_discounts_customers()
    print("Done with Proportion of discounted and undiscounted orders of top 10 customers")
    print("Time taken", dt.datetime.now() - begin_time)
    return result


""" Proportion of orders with and without discounts """


@app.get("/discounted-transactions")
def proportion_discounted_transactions():
    begin_time = dt.datetime.now()
    print("Start getting proportion of orders with and without discounts")
    result = get_proportion_discounted_transactions()
    print("Done with proportion of orders with and without discounts")
    print("Time taken", dt.datetime.now() - begin_time)
    return result


if __name__ == "__main__":
    uvicorn.run("main:app")

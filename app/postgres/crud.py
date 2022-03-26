from collections import defaultdict
from pyrsistent import l

import sqlalchemy
from sqlalchemy import desc, asc
from sqlalchemy.orm import Session

import models
import schemas

""" Wrapper functions for get & create """


""" Class specific functions for get & create """


def get_revenue(db: Session, shop: str):
    return db.query(models.Revenue).filter(models.Revenue.shop == shop).order_by(asc(models.Revenue.date)).all()


def create_revenue(db: Session, revenue: schemas.RevenueCreate):
    db_revenue = models.Revenue(
        date=revenue.date, shop=revenue.shop, revenue=revenue.revenue)
    qry = db.query(models.Revenue).filter(models.Revenue.shop ==
                                          revenue.shop, models.Revenue.date == revenue.date).first()
    if qry is None:
        db.add(db_revenue)
        db.commit()
        db.refresh(db_revenue)
    else:
        qry.property = db_revenue
        db.merge(qry)
        db.commit()
    return db_revenue


def get_discounts(db: Session):
    return db.query(models.Discounts).all()


def create_discounts(db: Session, discounts: schemas.Discounts):
    db_discounts = models.Discounts(
        name=discounts.name, shop=discounts.shop, count=discounts.count)
    qry = db.query(models.Discounts).filter(models.Discounts.name == discounts.name,
                                            models.Discounts.shop == discounts.shop, models.Discounts.count == discounts.count).first()
    if qry is None:
        db.add(db_discounts)
        db.commit()
        db.refresh(db_discounts)
    else:
        qry.property = db_discounts
        db.merge(qry)
        db.commit()
    return db_discounts


""" Customers
"""


def get_customers(db: Session, shop):
    return db.query(models.Customers).filter(models.Customers.shop == shop).all()


def create_customers(db: Session, customers: schemas.Customers):
    db_customers = models.Customers(
        id=customers.id, shop=customers.shop, channel=customers.channel, name=customers.name, count=customers.count, total=customers.total)
    qry = db.query(models.Customers).filter(models.Customers.id == customers.id,
                                            models.Customers.shop == customers.shop).first()
    if qry is None:
        db.add(db_customers)
        db.commit()
        db.refresh(db_customers)
    else:
        qry.property = db_customers
        db.merge(qry)
        db.commit()
    return db_customers


""" Referrals
"""


def get_referrals(db: Session):
    return db.query(models.Referrals).all()


def create_referrals(db: Session, referrals: schemas.Referrals):
    db_referrals = models.Referrals(
        shop=referrals.shop, channel=referrals.channel, count=referrals.count, total=referrals.total)
    qry = db.query(models.Referrals).filter(models.Referrals.channel == referrals.channel,
                                            models.Referrals.shop == referrals.shop).first()
    if qry is None:
        db.add(db_referrals)
        db.commit()
        db.refresh(db_referrals)
    else:
        qry.property = db_referrals
        db.merge(qry)
        db.commit()
    return db_referrals

# def get_user_by_email(db: Session, email: str):
#     return db.query(models.User).filter(models.User.email == email).first()


# def get_users(db: Session, skip: int = 0, limit: int = 100):
#     return db.query(models.User).offset(skip).limit(limit).all()


# def get_items(db: Session, skip: int = 0, limit: int = 100):
#     return db.query(models.Item).offset(skip).limit(limit).all()


# def create_user_item(db: Session, item: schemas.ItemCreate, user_id: int):
#     db_item = models.Item(**item.dict(), owner_id=user_id)
#     db.add(db_item)
#     db.commit()
#     db.refresh(db_item)
#     return db_item

""" Lifetime Products
"""


def remove_lifetime_products(db: Session):
    db.execute("""DELETE FROM "lifetimeProducts";""")
    db.commit()

    return "Removed existing top lifetime products in database"


def create_lifetime_product(db: Session, lifetimeProduct: schemas.LifetimeProducts):
    db_product = models.LifetimeProducts(
        shop=lifetimeProduct.shop,
        product=lifetimeProduct.product,
        quantity=lifetimeProduct.quantity)
    db.add(db_product)
    db.commit()
    db.refresh(db_product)

    return db_product


def get_lifetime_products(db: Session, shop: str):
    return db.query(models.LifetimeProducts).filter(models.LifetimeProducts.shop == shop).all()


""" Filtered Products
"""


def remove_filtered_products(db: Session):
    db.execute("""DELETE FROM "filteredProducts";""")
    db.commit()

    return "Removed existing top filtered products in database"


def create_filtered_product(db: Session, filteredProduct: schemas.FilteredProducts):
    db_product = models.FilteredProducts(
        shop=filteredProduct.shop,
        product=filteredProduct.product,
        quantity=filteredProduct.quantity,
        past_days=filteredProduct.past_days)
    db.add(db_product)
    db.commit()
    db.refresh(db_product)

    return db_product


def get_filtered_products(db: Session, shop: str, past_days: int):
    return db.query(models.FilteredProducts).filter(models.FilteredProducts.shop == shop, models.FilteredProducts.past_days == past_days).all()


""" Source Orders
"""


def remove_source_orders(db: Session):
    db.execute("""DELETE FROM "sourceOrders";""")
    db.commit()


def create_source_order(db: Session, sourceOrder: schemas.SourceOrders):
    db_order = models.SourceOrders(
        shop=sourceOrder.shop,
        channel=sourceOrder.channel,
        quantity=sourceOrder.quantity)
    db.add(db_order)
    db.commit()
    db.refresh(db_order)

    return db_order


def obtain_source_orders(db: Session, shop: str):
    return db.query(models.SourceOrders).filter(models.SourceOrders.shop == shop).all()


def format_source_orders(source_orders, shop):
    formatted_orders = defaultdict(list)
    channel_list = []
    quantity_list = []
    for source_order in source_orders:
        channel_list.append(source_order.channel)
        quantity_list.append(source_order.quantity)
    formatted_orders[shop] = [channel_list, quantity_list]
    return formatted_orders


""" Revenues with and without discounts
"""


def remove_discounts_revenue(db: Session):
    db.execute("""DELETE FROM "discountsRevenue";""")
    db.commit()


def create_discounts_revenue(db: Session, discountsRevenue: schemas.DiscountsRevenue):
    db_discounts = models.DiscountsRevenue(
        shop=discountsRevenue.shop,
        category=discountsRevenue.category,
        amount=discountsRevenue.amount)
    db.add(db_discounts)
    db.commit()
    db.refresh(db_discounts)

    return db_discounts


def obtain_discounts_revenue(db: Session, shop: str):
    return db.query(models.DiscountsRevenue).filter(models.DiscountsRevenue.shop == shop).all()


def format_discounts_revenue(discounts_revenues, shop):
    formatted_discounts = defaultdict(list)
    category_list = []
    amount_list = []
    for discounts_revenue in discounts_revenues:
        category_list.append(discounts_revenue.category)
        amount_list.append(discounts_revenue.amount)
    formatted_discounts[shop] = [category_list, amount_list]
    return formatted_discounts


""" MBA Category Table 1
"""


def remove_mba_category1(db: Session):
    db.execute("""DELETE FROM "mbaCategory1";""")
    db.commit()


def create_mba_category1(db: Session, MBACategory1: schemas.MBACategory1):
    db_category1 = models.MBACategory1(
        category=MBACategory1.category,
        support_percentage=MBACategory1.support_percentage)
    db.add(db_category1)
    db.commit()
    db.refresh(db_category1)

    return db_category1


def obtain_mba_category1(db: Session):
    return db.query(models.MBACategory1).all()


def format_mba_category1(categories_list):
    category_list = []
    support_list = []
    for category_object in categories_list:
        category_list.append(category_object.category)
        support_list.append(category_object.support_percentage)
    return [category_list, support_list]


""" MBA Category Table 2
"""


def remove_mba_category2(db: Session):
    db.execute("""DELETE FROM "mbaCategory2";""")
    db.commit()


def create_mba_category2(db: Session, MBACategory2: schemas.MBACategory2):
    db_category2 = models.MBACategory2(
        antecedents=MBACategory2.antecedents,
        consequents=MBACategory2.consequents,
        lift=MBACategory2.lift)
    db.add(db_category2)
    db.commit()
    db.refresh(db_category2)

    return db_category2


def obtain_mba_category2(db: Session):
    return db.query(models.MBACategory2).all()


def format_mba_category2(categories_list):
    antecendents = []
    consequents = []
    lifts = []
    for category_object in categories_list:
        antecendents.append(category_object.antecedents)
        consequents.append(category_object.consequents)
        lifts.append(category_object.lift)
    return [antecendents, consequents, lifts]


""" MBA Crystal Table 1&2
"""
def delete_mba_crystal(db: Session, model):
    db.query(model).delete()
    db.commit()
    
def get_mba_crystal(db: Session, model, shop):
    return db.query(model).filter(model.shop==shop).all()

def create_mba_crystal1(db: Session, support, crystal, shop):
    db_crystal1 = models.MBAcrystal1(crystal=crystal,
                                     support=support,
                                     shop=shop)
    db.add(db_crystal1)
    db.commit()

    return "Done"

def create_mba_crystal2(db: Session, antecedents,
                        consequents,
                        lift,
                        shop):
    db_crystal2 = models.MBACrystal2(antecedents=antecedents,
                                     consequents=consequents,
                                     lift=lift,
                                     shop=shop)
    db.add(db_crystal2)
    db.commit()

    return "Done"

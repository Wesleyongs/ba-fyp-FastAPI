from typing import Optional

from pydantic import BaseModel

import datetime as dt


class RevenueBase(BaseModel):
    date: dt.date
    shop: str
    revenue: int


class RevenueCreate(RevenueBase):
    pass


class Revenue(RevenueBase):

    class Config:
        orm_mode = True


class Discounts(BaseModel):
    name: str
    shop: str
    count: int

    class Config:
        orm_mode = True


class LifetimeProducts(BaseModel):
    shop: str
    product: str
    quantity: int

    class Config:
        orm_mode = True


class FilteredProducts(BaseModel):
    shop: str
    product: str
    quantity: int
    past_days: int

    class Config:
        orm_mode = True


class Customers(BaseModel):
    name: str
    shop: str
    channel: str
    count: int
    total: int
    id: int

    class Config:
        orm_mode = True


class Referrals(BaseModel):
    channel: str
    shop: str
    total: int
    count: int
    class Config:
        orm_mode = True


class SourceOrders(BaseModel):
    shop: str
    channel: str
    quantity: int
    class Config:
        orm_mode = True


class DiscountsRevenue(BaseModel):
    shop: str
    category: str
    amount: float

    class Config:
        orm_mode = True


class MBACategory1(BaseModel):
    number: int
    category: list
    support_percentage = float

    class Config:
        orm_mode = True


class MBACategory2(BaseModel):
    number: int
    antecedents: list
    consequents: list
    lift: float

    class Config:
        orm_mode = True
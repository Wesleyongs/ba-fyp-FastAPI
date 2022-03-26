from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Date, Float
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from database import Base


class Revenue(Base):
    __tablename__ = "revenue"

    date = Column(Date, primary_key=True, index=True)
    shop = Column(String, primary_key=True, index=True)
    revenue = Column(Integer)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class Discounts(Base):
    __tablename__ = "discounts"

    shop = Column(String, primary_key=True, index=True)
    name = Column(String, primary_key=True, index=True)
    count = Column(Integer)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class Customers(Base):
    __tablename__ = "topCustomers"

    id = Column(Integer, primary_key=True, index=True)
    shop = Column(String, primary_key=True, index=True)
    name = Column(String)
    channel = Column(String)
    count = Column(Integer)
    total = Column(Integer)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class Referrals(Base):
    __tablename__ = "referrals"

    channel = Column(String, primary_key=True, index=True)
    shop = Column(String, primary_key=True, index=True)
    count = Column(Integer)
    total = Column(Integer)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class LifetimeProducts(Base):
    __tablename__ = "lifetimeProducts"

    shop = Column(String, primary_key=True, index=True)
    product = Column(String, primary_key=True, index=True)
    quantity = Column(Integer)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class FilteredProducts(Base):
    __tablename__ = "filteredProducts"

    shop = Column(String, primary_key=True, index=True)
    product = Column(String, primary_key=True, index=True)
    past_days = Column(Integer, primary_key=True, index=True)
    quantity = Column(Integer)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class SourceOrders(Base):
    __tablename__ = "sourceOrders"

    shop = Column(String, primary_key=True, index=True)
    channel = Column(String, primary_key=True, index=True)
    quantity = Column(Integer)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class DiscountsRevenue(Base):
    __tablename__ = "discountsRevenue"

    shop = Column(String, primary_key=True, index=True)
    category = Column(String, primary_key=True, index=True)
    amount = Column(Float)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class MBACategory1(Base):
    __tablename__ = "mbaCategory1"

    number = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(postgresql.ARRAY(String, dimensions=1), index=True)
    support_percentage = Column(Float)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class MBACategory2(Base):
    __tablename__ = "mbaCategory2"

    number = Column(Integer, primary_key=True, autoincrement=True)
    antecedents = Column(postgresql.ARRAY(String, dimensions=1), index=True)
    consequents = Column(postgresql.ARRAY(String, dimensions=1), index=True)
    lift = Column(Float)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class MBAcrystal1(Base):
    __tablename__ = "mbaCrystal1"

    crystal = Column(String, primary_key=True)
    shop = Column(String, primary_key=True)
    support = Column(Float)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class MBACrystal2(Base):
    __tablename__ = "mbaCrystal2"

    antecedents = Column(String, primary_key=True)
    consequents = Column(String, primary_key=True)
    lift = Column(Float)
    shop = Column(String, primary_key=True)

    def __init__(self, **kwds):
        self.__dict__.update(kwds)

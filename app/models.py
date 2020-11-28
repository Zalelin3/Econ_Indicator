# models.py
from app import db
# from dataclasses import dataclass

# @dataclass
class Trade(db.Model):
    # icode:str
    # rcode:str
    # pcode:str
    # procode:str
    # year:int
    # frequency:str
    # unitcode:str
    # valueflagcode:str
    # valueflag:str
    # growthrate:float
    # percentage:float
    __tablename__ = 'trade'
    icode =db.Column(db.String(5),nullable = False,primary_key=True)
    rcode =db.Column(db.String(5),nullable = False,primary_key=True)
    pcode = db.Column(db.String(5),nullable = False,primary_key=True)
    procode = db.Column(db.String(10),nullable = False,primary_key=True)
    year = db.Column(db.SmallInteger,nullable = False,primary_key=True)
    frequency = db.Column(db.String(6),nullable = False)
    unitcode = db.Column(db.String(5),nullable = False)
    unit = db.Column(db.String(20),nullable = False)
    valueflagcode = db.Column(db.String(5), default= None)
    valueflag = db.Column(db.String(20), default=None)
    tradevalue = db.Column(db.Integer, nullable =False)
    growthrate = db.Column(db.Float(5,4),default=None)
    percentage = db.Column(db.Float(5,5),default= None)

# @dataclass
class Economies(db.Model):
    #
    # ecode:str
    # ename:str
    # iso3a:str
    # is_group:int
    # is_region:int

    __tablename__ = 'economies'
    ecode = db.Column(db.String(5),primary_key=True)
    ename = db.Column(db.String(100),nullable=False)
    iso3a = db.Column(db.String(6), default=None)
    is_group = db.Column(db.Boolean, nullable=False)
    is_region = db.Column(db.Boolean, nullable=False)
    is_reporter = db.Column(db.Boolean, nullable=False)
    is_partner = db.Column(db.Boolean, nullable=False)

class Indicators(db.Model):

    __tablename__ = "indicators"
    icode = db.Column(db.String(10), primary_key=True)
    category = db.Column(db.String(60), nullable= False)
    indicator = db.Column(db.String(100), default = None)

class Products(db.Model):
    __tablename__="products"
    pcode = db.Column(db.String(10), primary_key=True)
    product = db.Column(db.String(70), nullable=False)
    classification_code = db.Column(db.String(10), nullable=False)
    classification = db.Column(db.String(70), default=None)
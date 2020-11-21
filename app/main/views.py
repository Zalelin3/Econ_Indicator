# app/main/views.py

from flask import render_template, request
from . import main
import json
from .. import db
from ..models import Economies, Indicators, Products


@main.route('/')
def hello():
    """

    :return: Initialize the search dashboard page
    """
    econs = Economies.query.order_by(Economies.ename).all()
    prods = Products.query.order_by(Products.product).all()
    years = """
            select min(year) as start, max(year) as end from trade;
            """
    year = db.session.execute(years).first()
    start = year.start
    end = year.end
    return render_template('main.html', title="Economic Trade Indicator",
                           econs=econs, prods=prods, start=start, end=end)


@main.route('/info')
def info():
    """

    :return: Initialize the Technical Information Page I
    """
    econs = Economies.query.order_by(Economies.ename).all()
    indicators = Indicators.query.order_by(Indicators.category, Indicators.indicator).all()
    products = Products.query.order_by(Products.classification_code, Products.pcode).all()
    return render_template('info.html', title="Technical Sheet", econs=econs,
                           indicators=indicators, products=products)


# @main.route('/reporter', methods=['GET'])
# def reporter():
#     rcodes = Economies.query.order_by(Economies.ename).all()
#     # res = jsonify(rcodes)
#     res = []
#     for _ in rcodes:
#         temp = _.__dict__
#         temp.pop('_sa_instance_state', None)
#         res.append(temp)
#     return json.dumps({"partner":res})


@main.route('/filterCategory/<id1>/<id2>/<id3>', methods=['GET'])
def filterCategory(id1, id2, id3):
    """
        Filter the Indicator by the Reporter Economy and Partner Economy
        :param id1: reporting economy code, string
        :param id2: partner economy code,string
        :param id3: indicator code, string
        :return: a json object which contains a list of procode
         which satisfies the condition
        """
    if id1 == "-1" or id2 == "-1":
        query = """
                select distinct procode
                from trade
                where icode =:id3"""
        procodes = db.session.execute(query, {"id3": id3}).fetchall()
    else:
        query = """
                select distinct procode
                from trade
                where rcode =:id1 and pcode =:id2 and icode =:id3"""
        procodes = db.session.execute(query, {"id1": id1, "id2": id2, "id3": id3}).fetchall()
    return json.dumps([_.procode for _ in procodes])


@main.route('/filterIndicator/<id1>/<id2>', methods=['GET'])
def filterIndicator(id1, id2):
    """
        Filter the Indicator by the Reporter Economy and Partner Economy
        :param id1: reporting economy code, string
        :param id2: partner economy code, string
        :return: a json object which contains a list of icode
         which satisfies the condition
        """
    query = """
            select distinct icode
            from trade
            where rcode =:id1 and pcode =:id2"""
    icodes = db.session.execute(query, {"id1": id1, "id2": id2}).fetchall()
    return json.dumps([_.icode for _ in icodes])


@main.route('/filterPartner/<id>', methods=['GET'])
def filterPartner(id):
    """
        Filter the partner Economy by selected reporting Economy
        :param id: reporting economy code, string
        :return: a json object which contains a list of ecode
         which satisfies the condition
        """
    query = """
            select distinct pcode
            from trade
            where rcode =:id"""
    econs = db.session.execute(query, {"id": id}).fetchall()
    return json.dumps([_.pcode for _ in econs])


@main.route('/filterReporting/<id1>/<id2>', methods=['GET'])
def filterReporting(id1, id2):
    """
    Filter the reporting Economy by selected conditions on
    is_group and is_region
    :param id1: conditional selector on reporting economy, string
    :param id2: partner economy code, string
    :return: a json object which contains a list of ecode
     which satisfies the condition
    """

    if id1 == '0':
        econs = Economies.query.filter_by(is_group=0).all()
    elif id1 == '1':
        econs = Economies.query.filter_by(is_group=1).all()
    elif id1 == '2':
        econs = Economies.query.filter_by(is_region=1).all()
    else:
        econs = []  # id = -1 which is handled by javascript's function
    if id2 == "-1":
        res = [_.ecode for _ in econs]
    else:
        temp = [_.ecode for _ in econs]
        query = """
                    select distinct rcode
                    from trade
                    where pcode =:id"""
        rids = db.session.execute(query, {"id": id2}).fetchall()
        res = []
        for _ in rids:
            if (_.rcode in temp) or (id1 == "-1"):
                res.append(_.rcode)
    # print(len(res))
    return json.dumps(res)


@main.route('/search', methods=['POST'])
def search():
    """
    :return: Return search results through json format
    """
    flag1 = 0  # for economy
    flag2 = 0  # for time range
    flag3 = 1  # for indicator
    flag4 = 1  # for category
    if request.values['text'] != '':
        q1 = """
             select ecode, ename from economies
             where lower(ecode) = lower(:id)
             or lower(ename) = lower(:id)
             or lower(iso3a) = lower(:id)"""
        q2 = """
             select icode, indicator from indicators
             where lower(icode) = lower(:id)
             """
        q3 = """
             select pcode, product from products
             where lower(pcode) = lower(:id)
             """

        parse_list = [_.strip() for _ in request.values['text'].split(';')]
        for item in parse_list:
            if flag1 != 2:
                try:
                    temp = db.session.execute(q1, {'id': item}).first()
                    if flag1 == 0:
                        rid = temp.ecode
                        reporter = temp.ename
                        flag1 += 1
                    else:
                        pid = temp.ecode
                        partner = temp.ename
                        flag1 = 2
                    continue
                except:
                    pass
            if flag2 != 2:
                try:
                    if int(item) >= 1949 and int(item) <= 2019:
                        if flag2 == 0:
                            syear = int(item)
                            flag2 += 1
                            continue
                        else:
                            if int(item) < syear:
                                eyear = syear
                                syear = int(item)
                            else:
                                eyear = int(item)
                            flag2 = 2
                            continue
                except:
                    pass
            if flag3:
                try:
                    temp = db.session.execute(q2, {'id': item}).first()
                    icode = temp.icode
                    indicator = temp.indicator
                    flag3 = 0
                    continue
                except:
                    pass
            if flag4:
                try:
                    temp = db.session.execute(q3, {'id': item}).first()
                    procode = temp.pcode
                    category = temp.product
                    flag4 = 0
                    continue
                except:
                    pass

    if flag1 < 2:
        if flag1 == 0:
            rid = request.values['rid']
            try:
                reporter = Economies.query.filter_by(ecode=rid).first().ename
            except:
                return json.dumps({"num": 0, "error": "Incomplete Request: Need to select a reporting Economy"})
        pid = request.values['pid']
        try:
            partner = Economies.query.filter_by(ecode=pid).first().ename
        except:
            return json.dumps({"num": 0, "error": "Incomplete Request: Need to select a partner Economy"})
    if flag2 == 0:
        syear = request.values['syear']
        eyear = request.values['eyear']
        if syear == '-1':
            syear = '1949'
        if eyear == '-1':
            eyear = '2019'
    if flag2 == 1:
        eyear = syear

    if flag3:
        icode = request.values['icode']
        try:
            indicator = Indicators.query.filter_by(icode=icode).first().indicator
        except:
            return json.dumps({"num": 0, "error": "Incomplete Request: Need to select an indicator"})
    if flag4:
        procode = request.values['procode']
        try:
            category = Products.query.filter_by(pcode=procode).first().product
        except:
            return json.dumps({"num": 0, "error": "Incomplete Request: Need to select a category"})

    query = """
            select year, tradevalue, percentage, growthrate
            from trade
            where icode=:id1 and rcode=:id2 and pcode=:id3
                and procode=:id4 and year between :id5 and :id6
            order by year
            """
    res = db.session.execute(query, {'id1': icode, 'id2': rid,
                                     'id3': pid, 'id4': procode,
                                     'id5': syear, 'id6': eyear}).fetchall()

    rows = 0
    num_result = 0
    rows = [dict(_) for _ in res]
    num_result = len(rows)
    return json.dumps({"num": num_result, "indicator": indicator,
                       "reporter": reporter, "partner": partner,
                       "category": category, "rows": rows,
                       "error": 0})

#Author: Shihao Lin

from pyspark.sql import SparkSession
import mysql.connector
import pyspark.sql.functions as fc


if __name__ == "__main__":
    # Observe the progress by loading http://127.0.0.1:4040/
    spark = SparkSession\
        .builder\
        .appName("transformer")\
        .getOrCreate()
    
    db = mysql.connector.connect(
        host="localhost",
        user="econ",
        password="econ",
        database="trade_db",
        )
    
    cursor = db.cursor()

    # only merchandise
    merc = spark.read.csv('merchandise_values_annual_dataset.csv',header='true')

    # only service
    service = spark.read.csv('services_annual_dataset.csv',header='true')

    """
    Create Indicator table
    """
    # Create Table in MySQL
    # Indicator(icode, Category, Indicator)
    cursor.execute('DROP TABLE IF EXISTS indicators;')
    cursor.execute("create TABLE indicators (\
                    icode  VarChar(10) UNIQUE NOT NULL,\
                    category VarChar(60) NOT NULL,\
                    indicator VarChar(100),\
                    PRIMARY KEY (icode));")

    index_merc = merc.selectExpr("IndicatorCode icode", "IndicatorCategory category", "Indicator indicator").dropDuplicates()
    index_service = service.selectExpr("IndicatorCode icode", "IndicatorCategory category", "Indicator indicator").dropDuplicates()

    index = index_merc.unionByName(index_service).dropDuplicates()

    index.write.mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/trade_db") \
        .option("dbtable", "trade_db.indicators") \
        .option("user", "econ") \
        .option("password", "econ") \
        .save()
    del index
    
    """
    Create Product table
    """
    # Create Table in MySQL
    # Products(pcode, Product, Classification, ClassificationCode)
    cursor.execute('DROP TABLE IF EXISTS products;')
    cursor.execute("create TABLE products (\
                    pcode  VarChar(10) UNIQUE NOT NULL,\
                    product VarChar(70) NOT NULL,\
                    classification_code VarChar(10) NOT NULL,\
                    classification VarChar(70),\
                    PRIMARY KEY (pcode));")

    merc_category = merc.selectExpr('ProductCode pcode', 'Product product','ProductClassificationCode classification_code',\
        'ProductClassification classification').dropDuplicates()

    service_category = service.selectExpr('ProductCode pcode', 'Product product','ProductClassificationCode classification_code',\
        'ProductClassification classification').dropDuplicates()

    product = merc_category.unionByName(service_category).dropDuplicates()

    product.write.mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/trade_db") \
        .option("dbtable", "trade_db.products") \
        .option("user", "econ") \
        .option("password", "econ") \
        .save()
    del product

    """
    Create Economies table
    """
    # Create Table in MySQL
    # Economies Table(Code, Name, ISO3A,Is_Reporter, Is_Partner, Is_Group, Is_Region)
    cursor.execute('DROP TABLE IF EXISTS economies;')
    cursor.execute("create TABLE economies (\
                    ecode  VarChar(5) UNIQUE NOT NULL,\
                    ename VarChar(100) UNIQUE NOT NULL,\
                    iso3a VarChar(6),\
                    is_reporter boolean NOT NULL,\
                    is_partner boolean NOT NULL,\
                    is_region boolean Not NULL,\
                    is_group boolean Not NULL,\
                    PRIMARY KEY (ecode));")

    Region = ['Africa','Asia','Commonwealth of Independent States (CIS), including associate and former member States',
            'Europe','Middle East', 'North America', 'South and Central America and the Caribbean']
    GroupEconomic = ['World', 'Africa', 'African, Caribbean and Pacific States (ACP)',
        'Africa, CIS and Middle East', 'Andean Community (ANDEAN)', 'Asia',
        'Asia-Pacific Economic Cooperation (APEC)',
        'Association of Southeast Asian Nations (ASEAN)',
        'Australia and New Zealand', 'BRIC members', 'BRICS members',
        'Caribbean Community (CARICOM)',
        'Central African Economic and Monetary Community (CAEMC)',
        'Central American Common Market (CACM)',
        'Common Market for Eastern and Southern Africa (COMESA)',
        'Commonwealth of Independent States (CIS), including associate and former member States',
        'Euro Area (19)','Extra Euro Area (19) Trade', 'Europe',
        'European Free Trade Association (EFTA)', 'European Union',
        'European Union (28)', 'Four East Asian traders', 'G-20',
        'Gulf Cooperation Council (GCC)', 'LDC exporters of agriculture',
        'LDC exporters of manufactures', 'LDC non-fuel mineral exporters',
        'LDC oil exporters', 'Least-developed countries', 'Middle East',
        'Non-EU south-eastern Europe', 'Non-EU western Europe',
        'North America', 'North American Free Trade Agreement (NAFTA)',
        'Other Africa', 'Other Asia', 'Other CIS', 'Pacific Alliance',
        'Six East Asian traders',
        'South and Central America and the Caribbean',
        'South Asian Association for Regional Cooperation (SAARC)',
        'Southern African Development Community (SADC)',
        'Southern Common Market (MERCOSUR)',
        'Southern Common Market (MERCOSUR) excluding Venezuela, Bolivarian Republic of',
        'West African Economic and Monetary Union (WAEMU)',
        'West African Economic Community (ECOWAS)', 'WTO Members',
        'WTO Observer governments']
    

    merc_reporter = merc.selectExpr('ReporterCode ecode', 'Reporter ename','ReporterISO3A iso3a').dropDuplicates()
    merc_partner = merc.selectExpr('PartnerCode ecode', 'Partner ename','PartnerISO3A iso3a').dropDuplicates()

    service_reporter = service.selectExpr('ReporterCode ecode', 'Reporter ename','ReporterISO3A iso3a').dropDuplicates()

    # using Regex to check whether ecode is integer
    service_reporter = service_reporter.select(fc.when(service_reporter.ecode.rlike('^[0-9]+'), service_reporter.ecode.cast('int').cast('string')).
                            otherwise(service_reporter.ecode).alias('ecode'), service_reporter.ename, service_reporter.iso3a)

    service_partner = service.selectExpr('PartnerCode ecode', 'Partner ename','PartnerISO3A iso3a').dropDuplicates()
    service_partner = service_partner.select(fc.when(service_partner.ecode.rlike('^[0-9]+'), service_partner.ecode.cast('int').cast('string'))\
                    .otherwise(service_partner.ecode).alias('ecode'),service_partner.ename,service_partner.iso3a)

    reporter = merc_reporter.unionByName(service_reporter).dropDuplicates(['ecode'])
    reporter = reporter.select('*', fc.lit(1).alias('is_reporter'))
    partner = merc_partner.unionByName(service_partner).dropDuplicates(['ecode'])
    partner = partner.select('*', fc.lit(1).alias('is_partner'))
    
    # using outer join to create attribute is_reporter and is_partner
    econ = reporter.join(partner, reporter.ecode == partner.ecode, 'outer')\
        .select(fc.when(reporter.ecode.isNull(),partner.ecode).otherwise(reporter.ecode).alias("ecode"),
            fc.when(reporter.ename.isNull(),partner.ename).otherwise(reporter.ename).alias("ename"),
            fc.when(reporter.ecode.isNull(),partner.iso3a).otherwise(reporter.iso3a).alias("iso3a"),
            fc.when(reporter.is_reporter.isNull(), fc.lit(0)).otherwise(reporter.is_reporter).alias("is_reporter"),
            fc.when(partner.is_partner.isNull(), fc.lit(0)).otherwise(partner.is_partner).alias("is_partner"))
    
    econ = econ.select('*',fc.when(econ.ename.isin(Region), fc.lit(1)).otherwise(fc.lit(0)).alias("is_region"),
                        fc.when(econ.ename.isin(GroupEconomic), fc.lit(1)).otherwise(fc.lit(0)).alias('is_group'))



    econ.write.mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/trade_db") \
        .option("dbtable", "trade_db.economies") \
        .option("user", "econ") \
        .option("password", "econ") \
        .save()

    """
    Create Trade table
    """
    # Create Table in MySQL
    # TradeValues(Icode, Rcode, Pcode, Procode, Year, Frequency, Unit_Code,\
    #  Unit, ValueFlagCode, ValueFlag,tradevalue, growth_rate, percentage)
    cursor.execute('DROP TABLE IF EXISTS trade;')
    cursor.execute("create TABLE trade (\
                    icode  VarChar(10) NOT NULL,\
                    rcode  VarChar(5) NOT NULL,\
                    pcode  VarChar(5) NOT NULL,\
                    procode VarChar(10) NOT NULL,\
                    year SMALLINT UNSIGNED NOT NULL CHECK ( 1950 <year <2050 ),\
                    frequency VarChar(6) NOT NULL,\
                    unitcode VarChar(5) NOT NULL,\
                    unit VarChar(20) NOT NULL,\
                    valueflagcode VarChar(5),\
                    valueflag VarChar(20),\
                    tradevalue INT NOT NULL,\
                    growthrate FLOAT(8,4),\
                    percentage double,\
                    PRIMARY KEY (icode, rcode, pcode, procode, year),\
                    FOREIGN KEY (icode) REFERENCES trade_db.indicators(icode),\
                    FOREIGN KEY (rcode) REFERENCES trade_db.economies(ecode),\
                    FOREIGN KEY (pcode) REFERENCES trade_db.economies(ecode),\
                    FOREIGN KEY (procode) REFERENCES trade_db.products(pcode));")

    # merchandise part
    merc_trade = merc.selectExpr( "IndicatorCode icode", "ReporterCode rcode","PartnerCode pcode", "ProductCode procode", \
                            "cast(Year as int) year", "Frequency frequency","UnitCode unitcode", "Unit unit", \
                            "ValueFlagCode valueflagcode", "ValueFlag valueflag", "cast(Value as int) tradevalue")

    mtv_ax = merc_trade.where("icode='ITS_MTV_AX'")
    mtv_ax.createOrReplaceTempView("t1")
    mtv_ax.createOrReplaceTempView("t2")
    mtv_ax.createOrReplaceTempView("t3")

    mtv_ax = spark.sql("""select t.*, round((t.tradevalue/t3.tradevalue),5) percentage
                            from (select t1.*, round((t1.tradevalue -t2.tradevalue)/t2.tradevalue,4) growthrate
                                    from t1 left join t2 on t1.icode = t2.icode and t1.rcode= t2.rcode
                                    and t1.pcode=t2.pcode and t1.procode = t2.procode 
                                    and (t1.year-1 = t2.year)) t left join t3 on t.rcode = t3.rcode
                                    and t.pcode = t3.pcode and t.year = t3.year and t3.procode = 'TO'
                        """)

    # store merchandise export trade data into spark
    mtv_ax.write.mode('append') \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/trade_db") \
        .option("dbtable", "trade_db.trade") \
        .option("user", "econ") \
        .option("password", "econ") \
        .save()

    mtv_am = merc_trade.where("icode='ITS_MTV_AM'")
    mtv_am.createOrReplaceTempView("t1")
    mtv_am.createOrReplaceTempView("t2")
    mtv_am.createOrReplaceTempView("t3")

    mtv_am = spark.sql("""select t.*, round((t.tradevalue/t3.tradevalue),5) percentage
                            from (select t1.*, round((t1.tradevalue -t2.tradevalue)/t2.tradevalue,5) growthrate
                                    from t1 left join t2 on t1.icode = t2.icode and t1.rcode= t2.rcode
                                    and t1.pcode=t2.pcode and t1.procode = t2.procode 
                                    and (t1.year-1 = t2.year)) t left join t3 on t.rcode = t3.rcode
                                    and t.pcode = t3.pcode and t.year = t3.year and t3.procode = 'TO'
                        """)
    # store merchandise import trade data into spark
    mtv_am.write.mode('append') \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/trade_db") \
        .option("dbtable", "trade_db.trade") \
        .option("user", "econ") \
        .option("password", "econ") \
        .save()

    # trade in services part
    trade_service = service.selectExpr( "IndicatorCode icode", "ReporterCode rcode","PartnerCode pcode", "ProductCode procode", \
                                        "ProductClassificationCode clcode", "cast(Year as int) year", "Frequency frequency","UnitCode unitcode", \
                                        "Unit unit", "ValueFlagCode valueflagcode", "ValueFlag valueflag", "cast(Value as int) tradevalue")
    

    service_list = [('ITS_CS_AM5','BOP5','S200'),('ITS_CS_AM5','BOP6','S'),('ITS_CS_AM6','BOP5','S200'),('ITS_CS_AM6','BOP6','S'), \
                    ('ITS_CS_AX5','BOP5','S200'),('ITS_CS_AX5','BOP6','S'),('ITS_CS_AX6','BOP5','S200'),('ITS_CS_AX6','BOP6','S')]
    
    for n, values in enumerate (service_list):
        temp = trade_service.where(f"icode='{values[0]}' and clcode='{values[1]}'")
        temp.createOrReplaceTempView("t1")
        temp.createOrReplaceTempView("t2")
        temp.createOrReplaceTempView("t3")
        temp = spark.sql(f"""select t.icode, CASE WHEN t.rcode regexp "^[0-9]+$" THEN CAST(CAST(t.rcode AS Integer) AS String) ELSE t.rcode END rcode,
                            CASE WHEN t.pcode regexp "^[0-9]+$" THEN CAST(CAST(t.pcode AS Integer) AS String) ELSE t.pcode END pcode, t.procode, t.year,
                            t.frequency, t.unitcode, t.unit, t.valueflagcode, t.valueflag, t.tradevalue, t.growthrate,
                            round((t.tradevalue/t3.tradevalue),5) percentage
                            from (select t1.*, round((t1.tradevalue -t2.tradevalue)/t2.tradevalue,5) growthrate
                                    from t1 left join t2 on t1.icode = t2.icode and t1.rcode= t2.rcode
                                    and t1.pcode=t2.pcode and t1.procode = t2.procode 
                                    and (t1.year-1 = t2.year)) t left join t3 on t.rcode = t3.rcode
                                    and t.pcode = t3.pcode and t.year = t3.year and t3.procode = '{values[2]}'
                        """)

        # store trade in services data into spark
        temp.write.mode('append') \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/trade_db") \
            .option("dbtable", "trade_db.trade") \
            .option("user", "econ") \
            .option("password", "econ") \
            .save()

    cursor.execute('create index IDX_Rcode_Pcode_Icode_Procode On trade(rcode,pcode,icode,procode);')

    cursor.close()
    spark.stop()
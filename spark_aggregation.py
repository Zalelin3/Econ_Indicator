#Author: Shihao Lin

from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("create_growthrate_percentage")\
        .getOrCreate()

    # only merchandise

    merc = spark.read.format('csv').options(header='true').load('merchandise_values_annual_dataset.csv')

    merc = merc.selectExpr( "IndicatorCode icode", "ReporterCode rcode","PartnerCode pcode", "ProductCode procode", \
                            "cast(Year as int) year", "Frequency frequency","UnitCode unitcode", "Unit unit", \
                            "ValueFlagCode valueflagcode", "ValueFlag valueflag", "cast(Value as int) tradevalue")

    mtv_ax = merc.where("icode='ITS_MTV_AX'")
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
    mtv_ax.write.mode('append') \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/trade_db") \
        .option("dbtable", "trade_db.trade2") \
        .option("user", "econ") \
        .option("password", "econ") \
        .save()

    mtv_am = merc.where("icode='ITS_MTV_AM'")
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

    mtv_am.write.mode('append') \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/trade_db") \
        .option("dbtable", "trade_db.trade2") \
        .option("user", "econ") \
        .option("password", "econ") \
        .save()

    # only service
    service = spark.read.format('csv').options(header='true').load('services_annual_dataset.csv')

    service = service.selectExpr( "IndicatorCode icode", "ReporterCode rcode","PartnerCode pcode", "ProductCode procode", \
                "ProductClassificationCode clcode", "cast(Year as int) year", "Frequency frequency","UnitCode unitcode", \
                     "Unit unit", "ValueFlagCode valueflagcode", "ValueFlag valueflag", "cast(Value as int) tradevalue")
    
    service_list = [('ITS_CS_AM5','BOP5','S200'),('ITS_CS_AM5','BOP6','S'),('ITS_CS_AM6','BOP5','S200'),('ITS_CS_AM6','BOP6','S'), \
                    ('ITS_CS_AX5','BOP5','S200'),('ITS_CS_AX5','BOP6','S'),('ITS_CS_AX6','BOP5','S200'),('ITS_CS_AX6','BOP6','S')]
    
    for n, values in enumerate (service_list):
        temp = service.where(f"icode='{values[0]}' and clcode='{values[1]}'")
        temp.createOrReplaceTempView("t1")
        temp.createOrReplaceTempView("t2")
        temp.createOrReplaceTempView("t3")
        temp = spark.sql(f"""select t.*, round((t.tradevalue/t3.tradevalue),5) percentage
                            from (select t1.*, round((t1.tradevalue -t2.tradevalue)/t2.tradevalue,5) growthrate
                                    from t1 left join t2 on t1.icode = t2.icode and t1.rcode= t2.rcode
                                    and t1.pcode=t2.pcode and t1.procode = t2.procode 
                                    and (t1.year-1 = t2.year)) t left join t3 on t.rcode = t3.rcode
                                    and t.pcode = t3.pcode and t.year = t3.year and t3.procode = '{values[2]}'
                        """).drop('clcode')

        temp.write.mode('append') \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/trade_db") \
            .option("dbtable", "trade_db.trade2") \
            .option("user", "econ") \
            .option("password", "econ") \
            .save()


    spark.stop()
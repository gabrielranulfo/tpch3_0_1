import os
import findspark
from pyspark.sql.functions import col, sum as spark_sum, mean, count, min as spark_min, desc, asc, expr, year, when, trim, avg, substring  
from pyspark.sql.types import DoubleType
from datetime import date

WORK_DIR = os.environ.get('WORK_DIR')
SCALE = os.environ.get("SCALE_FACTOR", "1")
N_CORES = int(os.environ.get("N_CORES", "1"))
dataset_path = f"data_tbl/scale-{SCALE}/"

#os.environ["JAVA_HOME"] = f"{WORK_DIR}/jdk-17.0.0.1/"
os.environ["SPARK_HOME"] = f"{WORK_DIR}/spark-4.0.1-bin-hadoop3/"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.17.0-openjdk-amd64/"

findspark.init(os.environ["SPARK_HOME"])

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TesteSpark") \
    .master(f"local[{N_CORES}]") \
    .getOrCreate()

def query_01(spark, dataset_path, SCALE):
    VAR1 = date(1998, 9, 2)
    
    # Ler apenas as colunas necessárias
    lineitem_ds = spark.read.parquet(os.path.join(dataset_path, "lineitem.parquet")) \
        .select(
            "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice",
            "l_discount", "l_tax", "l_orderkey", "l_shipdate"
        )
    
    result = lineitem_ds.filter(col("l_shipdate") <= VAR1) \
        .withColumn("disc_price", col("l_extendedprice") * (1 - col("l_discount"))) \
        .withColumn("charge", col("l_extendedprice") * (1 - col("l_discount")) * (1 + col("l_tax"))) \
        .groupBy("l_returnflag", "l_linestatus") \
        .agg(
            spark_sum("l_quantity").alias("sum_qty"),
            spark_sum("l_extendedprice").alias("sum_base_price"),
            spark_sum("disc_price").alias("sum_disc_price"),
            spark_sum("charge").alias("sum_charge"),
            mean("l_quantity").alias("avg_qty"),
            mean("l_extendedprice").alias("avg_price"),
            mean("l_discount").alias("avg_disc"),
            count("l_orderkey").alias("count_order")
        ) \
        .orderBy("l_returnflag", "l_linestatus")
    
    return result

result = query_01(spark, dataset_path, SCALE)
#result.show()

def query_02(spark, dataset_path, SCALE):
    var_1 = 15
    var_2 = "BRASS"
    var_3 = "EUROPE"

    # Ler os datasets
    region_ds = spark.read.parquet(os.path.join(dataset_path, "region.parquet"))
    nation_ds = spark.read.parquet(os.path.join(dataset_path, "nation.parquet"))
    supplier_ds = spark.read.parquet(os.path.join(dataset_path, "supplier.parquet"))
    part_ds = spark.read.parquet(os.path.join(dataset_path, "part.parquet"))
    part_supp_ds = spark.read.parquet(os.path.join(dataset_path, "partsupp.parquet"))

    # Subquery para custo mínimo
    min_cost_subquery = (
        part_supp_ds.join(supplier_ds, part_supp_ds.ps_suppkey == supplier_ds.s_suppkey)
        .join(nation_ds, supplier_ds.s_nationkey == nation_ds.n_nationkey)
        .join(region_ds, nation_ds.n_regionkey == region_ds.r_regionkey)
        .filter(col("r_name") == var_3)
        .groupBy("ps_partkey")
        .agg(spark_min("ps_supplycost").alias("min_cost"))
    )

    # Query principal
    result = (
        part_ds.join(part_supp_ds, part_ds.p_partkey == part_supp_ds.ps_partkey)
        .join(supplier_ds, part_supp_ds.ps_suppkey == supplier_ds.s_suppkey)
        .join(nation_ds, supplier_ds.s_nationkey == nation_ds.n_nationkey)
        .join(region_ds, nation_ds.n_regionkey == region_ds.r_regionkey)
        .filter(
            (col("p_size") == var_1) &
            (col("p_type").endswith(var_2)) &
            (col("r_name") == var_3)
        )
        .join(min_cost_subquery, 
              (part_ds.p_partkey == min_cost_subquery.ps_partkey) & 
              (part_supp_ds.ps_supplycost == min_cost_subquery.min_cost))
        .select(
            "s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr",
            "s_address", "s_phone", "s_comment"
        )
        .withColumn("s_name", col("s_name").cast("string"))
        .withColumn("n_name", col("n_name").cast("string"))
        .withColumn("p_mfgr", col("p_mfgr").cast("string"))
        .withColumn("s_address", col("s_address").cast("string"))
        .withColumn("s_phone", col("s_phone").cast("string"))
        .withColumn("s_comment", col("s_comment").cast("string"))
        .orderBy(desc("s_acctbal"), asc("n_name"), asc("s_name"), asc("p_partkey"))
        .limit(100)
    )
    
    return result

# Adicione esta chamada após a definição da função
result2 = query_02(spark, dataset_path, SCALE)
#result2.show()

def query_03(spark, dataset_path, SCALE):
    var_1 = var_2 = date(1995, 3, 15)
    var_3 = "BUILDING"

    # Ler os datasets
    customer_ds = spark.read.parquet(os.path.join(dataset_path, "customer.parquet"))
    line_item_ds = spark.read.parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = spark.read.parquet(os.path.join(dataset_path, "orders.parquet"))

    result = (
        customer_ds.filter(col("c_mktsegment") == var_3)
        .join(orders_ds, customer_ds.c_custkey == orders_ds.o_custkey)
        .join(line_item_ds, orders_ds.o_orderkey == line_item_ds.l_orderkey)
        .filter(
            (col("o_orderdate") < var_2) &
            (col("l_shipdate") > var_1)
        )
        .withColumn("revenue", col("l_extendedprice") * (1 - col("l_discount")))
        .groupBy("o_orderkey", "o_orderdate", "o_shippriority")
        .agg(spark_sum("revenue").alias("revenue"))
        .select(
            col("o_orderkey").alias("l_orderkey"),
            col("revenue"),
            col("o_orderdate"),
            col("o_shippriority")
        )
        .orderBy(col("revenue").desc(), col("o_orderdate").asc())
        .limit(10)
    )
    
    return result

# Adicione esta chamada após a definição da função
result3 = query_03(spark, dataset_path, SCALE)
#result3.show()

def query_04(spark, dataset_path):
    """
    TPC-H Query 4 - Order Priority Checking
    """
    var_1 = date(1993, 10, 1)
    var_2 = date(1994, 1, 1)
    
    # Read data
    lineitem_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")
    
    # Execute query
    result = (
        lineitem_df.join(orders_df, lineitem_df.l_orderkey == orders_df.o_orderkey)
        .filter(
            (col("o_orderdate") >= var_1) &
            (col("o_orderdate") < var_2) &
            (col("l_commitdate") < col("l_receiptdate"))
        )
        .select("o_orderpriority", "l_orderkey")
        .dropDuplicates(["o_orderpriority", "l_orderkey"])
        .groupBy("o_orderpriority")
        .agg(count("*").alias("order_count"))
        .orderBy("o_orderpriority")
    )
    
    return result

result4 = query_04(spark, dataset_path)
#result4.show()

def query_05(spark, dataset_path):
    var_1 = "ASIA"
    var_2 = date(1994, 1, 1)
    var_3 = date(1995, 1, 1)

    # Read data
    region_df = spark.read.parquet(f"{dataset_path}/region.parquet")
    nation_df = spark.read.parquet(f"{dataset_path}/nation.parquet")
    customer_df = spark.read.parquet(f"{dataset_path}/customer.parquet")
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")
    supplier_df = spark.read.parquet(f"{dataset_path}/supplier.parquet")

    result = (
        region_df.join(nation_df, region_df.r_regionkey == nation_df.n_regionkey)
        .join(customer_df, nation_df.n_nationkey == customer_df.c_nationkey)
        .join(orders_df, customer_df.c_custkey == orders_df.o_custkey)
        .join(line_item_df, orders_df.o_orderkey == line_item_df.l_orderkey)
        .join(supplier_df, 
              (line_item_df.l_suppkey == supplier_df.s_suppkey) & 
              (nation_df.n_nationkey == supplier_df.s_nationkey))
        .filter(
            (col("r_name") == var_1) &
            (col("o_orderdate") >= var_2) &
            (col("o_orderdate") < var_3)
        )
        .groupBy("n_name")
        .agg(expr("sum(l_extendedprice * (1 - l_discount))").alias("revenue"))
        .orderBy(col("revenue").desc())
    )
    
    return result

result5 = query_05(spark, dataset_path)
#result5.show()

def query_06(spark, dataset_path):
    var_1 = date(1994, 1, 1)
    var_2 = date(1995, 1, 1)
    var_3 = 0.05
    var_4 = 0.07
    var_5 = 24

    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")

    result = (
        line_item_df.filter(
            (col("l_shipdate") >= var_1) &
            (col("l_shipdate") < var_2) &
            (col("l_discount") >= var_3) &
            (col("l_discount") <= var_4) &
            (col("l_quantity") < var_5)
        )
        .select(expr("sum(l_extendedprice * l_discount)").alias("revenue"))
    )
    
    return result

result6 = query_06(spark, dataset_path)
#result6.show()

def query_07(spark, dataset_path):
    # Read data
    nation_df = spark.read.parquet(f"{dataset_path}/nation.parquet")
    customer_df = spark.read.parquet(f"{dataset_path}/customer.parquet")
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")
    supplier_df = spark.read.parquet(f"{dataset_path}/supplier.parquet")

    # Filter nations with aliases
    n1 = nation_df.filter(col("n_name") == "FRANCE").alias("n1")
    n2 = nation_df.filter(col("n_name") == "GERMANY").alias("n2")

    var_1 = date(1995, 1, 1)
    var_2 = date(1996, 12, 31)

    # Primeira combinação: FRANCE como cust_nation, GERMANY como supp_nation
    df1 = (
        customer_df.join(n1, customer_df.c_nationkey == col("n1.n_nationkey"))
        .join(orders_df, customer_df.c_custkey == orders_df.o_custkey)
        .join(line_item_df, orders_df.o_orderkey == line_item_df.l_orderkey)
        .join(supplier_df, line_item_df.l_suppkey == supplier_df.s_suppkey)
        .join(n2, supplier_df.s_nationkey == col("n2.n_nationkey"))
        .select(
            col("n2.n_name").alias("supp_nation"),
            col("n1.n_name").alias("cust_nation"),
            col("l_shipdate"),
            col("l_extendedprice"),
            col("l_discount")
        )
    )

    # Segunda combinação: GERMANY como cust_nation, FRANCE como supp_nation
    df2 = (
        customer_df.join(n2, customer_df.c_nationkey == col("n2.n_nationkey"))
        .join(orders_df, customer_df.c_custkey == orders_df.o_custkey)
        .join(line_item_df, orders_df.o_orderkey == line_item_df.l_orderkey)
        .join(supplier_df, line_item_df.l_suppkey == supplier_df.s_suppkey)
        .join(n1, supplier_df.s_nationkey == col("n1.n_nationkey"))
        .select(
            col("n1.n_name").alias("supp_nation"),
            col("n2.n_name").alias("cust_nation"),
            col("l_shipdate"),
            col("l_extendedprice"),
            col("l_discount")
        )
    )

    # Concatenar e processar
    result = (
        df1.union(df2)
        .filter(
            (col("l_shipdate") >= var_1) &
            (col("l_shipdate") <= var_2)
        )
        .withColumn("l_year", year(col("l_shipdate")))
        .groupBy("supp_nation", "cust_nation", "l_year")
        .agg(expr("sum(l_extendedprice * (1 - l_discount))").alias("revenue"))
        .orderBy("supp_nation", "cust_nation", "l_year")
        )
    
    return result

result7 = query_07(spark, dataset_path)
#result7.show() 

def query_08(spark, dataset_path):
    # Read data
    part_df = spark.read.parquet(f"{dataset_path}/part.parquet")
    supplier_df = spark.read.parquet(f"{dataset_path}/supplier.parquet")
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")
    customer_df = spark.read.parquet(f"{dataset_path}/customer.parquet")
    nation_df = spark.read.parquet(f"{dataset_path}/nation.parquet")
    region_df = spark.read.parquet(f"{dataset_path}/region.parquet")

    # Filter for AMERICA
    result = (
        part_df.join(line_item_df, part_df.p_partkey == line_item_df.l_partkey)
        .join(supplier_df, line_item_df.l_suppkey == supplier_df.s_suppkey)
        .join(orders_df, line_item_df.l_orderkey == orders_df.o_orderkey)
        .join(customer_df, orders_df.o_custkey == customer_df.c_custkey)
        .join(nation_df.alias("n1"), customer_df.c_nationkey == col("n1.n_nationkey"))
        .join(region_df, col("n1.n_regionkey") == region_df.r_regionkey)
        .join(nation_df.alias("n2"), supplier_df.s_nationkey == col("n2.n_nationkey"))
        .filter(
            (col("r_name") == "AMERICA") &
            (col("o_orderdate") >= date(1995, 1, 1)) &
            (col("o_orderdate") <= date(1996, 12, 31)) &
            (col("p_type") == "ECONOMY ANODIZED STEEL")
        )
        .withColumn("o_year", year(col("o_orderdate")))
        .withColumn("volume", col("l_extendedprice") * (1 - col("l_discount")))
        .withColumn("brazil_volume", 
                   when(col("n2.n_name") == "BRAZIL", col("volume")).otherwise(0))
        .groupBy("o_year")
        .agg(
            expr("sum(brazil_volume) / sum(volume)").alias("mkt_share")
        )
        .orderBy("o_year")
    )
    
    return result

result8 = query_08(spark, dataset_path)
#result8.show()

def query_09(spark, dataset_path):
    # Read data
    part_df = spark.read.parquet(f"{dataset_path}/part.parquet")
    supplier_df = spark.read.parquet(f"{dataset_path}/supplier.parquet")
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    part_supp_df = spark.read.parquet(f"{dataset_path}/partsupp.parquet")
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")
    nation_df = spark.read.parquet(f"{dataset_path}/nation.parquet")

    result = (
        line_item_df.join(supplier_df, line_item_df.l_suppkey == supplier_df.s_suppkey)
        .join(part_supp_df, 
              (line_item_df.l_suppkey == part_supp_df.ps_suppkey) & 
              (line_item_df.l_partkey == part_supp_df.ps_partkey))
        .join(part_df, line_item_df.l_partkey == part_df.p_partkey)
        .join(orders_df, line_item_df.l_orderkey == orders_df.o_orderkey)
        .join(nation_df, supplier_df.s_nationkey == nation_df.n_nationkey)
        .filter(col("p_name").contains("green"))
        .withColumn("o_year", year(col("o_orderdate")))
        .groupBy(col("n_name").alias("nation"), "o_year")
        .agg(expr("sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity)").alias("sum_profit"))
        .orderBy("nation", col("o_year").desc())
    )
    
    return result

result9 = query_09(spark, dataset_path)
#result9.show()

def query_10(spark, dataset_path):
    var_1 = date(1993, 10, 1)
    var_2 = date(1994, 1, 1)

    # Read data
    customer_df = spark.read.parquet(f"{dataset_path}/customer.parquet")
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    nation_df = spark.read.parquet(f"{dataset_path}/nation.parquet")

    result = (
        customer_df.join(orders_df, customer_df.c_custkey == orders_df.o_custkey)
        .join(line_item_df, orders_df.o_orderkey == line_item_df.l_orderkey)
        .join(nation_df, customer_df.c_nationkey == nation_df.n_nationkey)
        .filter(
            (col("o_orderdate") >= var_1) &
            (col("o_orderdate") < var_2) &
            (col("l_returnflag") == "R")
        )
        .withColumn("revenue", col("l_extendedprice") * (1 - col("l_discount")))
        .groupBy(
            "c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"
        )
        .agg(expr("sum(revenue)").alias("revenue"))
        .withColumn("c_address", trim(col("c_address")))
        .withColumn("c_comment", trim(col("c_comment")))
        .select(
            "c_custkey", "c_name", "revenue", "c_acctbal", "n_name", 
            "c_address", "c_phone", "c_comment"
        )
        .orderBy(col("revenue").desc())
        .limit(20)
    )
    
    return result

result10 = query_10(spark, dataset_path)
#result10.show()

def query_11(spark, dataset_path, scale):
    var_1 = "GERMANY"
    var_2 = 0.0001 / scale

    # Read data
    supplier_df = spark.read.parquet(f"{dataset_path}/supplier.parquet")
    part_supp_df = spark.read.parquet(f"{dataset_path}/partsupp.parquet")
    nation_df = spark.read.parquet(f"{dataset_path}/nation.parquet")

    # Primeira parte: join das tabelas e filtro por nação
    res_1 = (
        part_supp_df.join(supplier_df, part_supp_df.ps_suppkey == supplier_df.s_suppkey)
        .join(nation_df, supplier_df.s_nationkey == nation_df.n_nationkey)
        .filter(col("n_name") == var_1)
    )

    # Calcular valor total
    total_value = (
        res_1.select(expr("sum(ps_supplycost * ps_availqty)").alias("total"))
        .first()["total"] * var_2
    )

    # Agrupamento e filtro
    result = (
        res_1.groupBy("ps_partkey")
        .agg(expr("sum(ps_supplycost * ps_availqty)").alias("value"))
        .filter(col("value") > total_value)
        .select("ps_partkey", "value")
        .orderBy(col("value").desc())
    )
    
    return result

result11 = query_11(spark, dataset_path, int(SCALE))
#result11.show()

def query_12(spark, dataset_path):
    var_1 = "MAIL"
    var_2 = "SHIP"
    var_3 = date(1994, 1, 1)
    var_4 = date(1995, 1, 1)

    # Read data
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")

    result = (
        orders_df.join(line_item_df, orders_df.o_orderkey == line_item_df.l_orderkey)
        .filter(
            (col("l_shipmode").isin([var_1, var_2])) &
            (col("l_commitdate") < col("l_receiptdate")) &
            (col("l_shipdate") < col("l_commitdate")) &
            (col("l_receiptdate") >= var_3) &
            (col("l_receiptdate") < var_4)
        )
        .withColumn("high_line_count", 
                   when(col("o_orderpriority").isin(["1-URGENT", "2-HIGH"]), 1).otherwise(0))
        .withColumn("low_line_count",
                   when(~col("o_orderpriority").isin(["1-URGENT", "2-HIGH"]), 1).otherwise(0))
        .groupBy("l_shipmode")
        .agg(
            expr("sum(high_line_count)").alias("high_line_count"),
            expr("sum(low_line_count)").alias("low_line_count")
        )
        .orderBy("l_shipmode")
    )
    
    return result

result12 = query_12(spark, dataset_path)
#result12.show()

def query_13(spark, dataset_path):
    var_1 = "special"
    var_2 = "requests"

    # Read data
    customer_df = spark.read.parquet(f"{dataset_path}/customer.parquet")
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")

    # Filtrar orders onde o comentário NÃO contém as palavras
    orders_filtered = orders_df.filter(
        ~col("o_comment").contains(f"{var_1}.*{var_2}")
    )

    # Left join entre customer e orders filtrado
    result = (
        customer_df.join(orders_filtered, 
                       customer_df.c_custkey == orders_filtered.o_custkey, 
                       how="left")
        .groupBy("c_custkey")
        .agg(count("o_orderkey").alias("c_count"))
        .groupBy("c_count")
        .agg(count("c_custkey").alias("custdist"))
        .orderBy(col("custdist").desc(), col("c_count").desc())
    )
    
    return result

result13 = query_13(spark, dataset_path)
#result13.show()

def query_14(spark, dataset_path):
    var_1 = date(1995, 9, 1)
    var_2 = date(1995, 10, 1)

    # Read data
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    part_df = spark.read.parquet(f"{dataset_path}/part.parquet")

    result = (
        line_item_df.join(part_df, line_item_df.l_partkey == part_df.p_partkey)
        .filter(
            (col("l_shipdate") >= var_1) &
            (col("l_shipdate") < var_2)
        )
        .select(
            expr("""
                100.00 * sum(
                    CASE WHEN p_type LIKE 'PROMO%' 
                    THEN l_extendedprice * (1 - l_discount) 
                    ELSE 0 
                    END
                ) / sum(l_extendedprice * (1 - l_discount))
            """).alias("promo_revenue")
        )
    )
    
    return result

result14 = query_14(spark, dataset_path)
#result14.show()

def query_15(spark, dataset_path):
    var_1 = date(1996, 1, 1)
    var_2 = date(1996, 4, 1)

    # Read data
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    supplier_df = spark.read.parquet(f"{dataset_path}/supplier.parquet")

    # Criar a view revenue
    revenue_df = (
        line_item_df.filter(
            (col("l_shipdate") >= var_1) &
            (col("l_shipdate") < var_2)
        )
        .groupBy("l_suppkey")
        .agg(expr("sum(l_extendedprice * (1 - l_discount))").alias("total_revenue"))
        .withColumnRenamed("l_suppkey", "supplier_no")
    )

    # Encontrar o máximo total_revenue
    max_revenue = revenue_df.agg(expr("max(total_revenue)").alias("max_rev")).first()["max_rev"]

    # Juntar com supplier e filtrar
    result = (
        supplier_df.join(revenue_df, supplier_df.s_suppkey == revenue_df.supplier_no)
        .filter(col("total_revenue") == max_revenue)
        .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
        .orderBy("s_suppkey")
    )
    
    return result

result15 = query_15(spark, dataset_path)
#result15.show()

def query_16(spark, dataset_path):
    var_1 = "Brand#45"
    sizes = [49, 14, 23, 45, 19, 3, 36, 9]

    # Read data
    part_supp_df = spark.read.parquet(f"{dataset_path}/partsupp.parquet")
    part_df = spark.read.parquet(f"{dataset_path}/part.parquet")
    supplier_df = spark.read.parquet(f"{dataset_path}/supplier.parquet")

    # Filtrar suppliers com complaints
    supplier_complaints = supplier_df.filter(
        col("s_comment").contains("Customer.*Complaints")
    ).select("s_suppkey")

    result = (
        part_df.join(part_supp_df, part_df.p_partkey == part_supp_df.ps_partkey)
        .filter(
            (col("p_brand") != var_1) &
            (~col("p_type").like("MEDIUM POLISHED%")) &
            (col("p_size").isin(sizes)) &
            (~col("ps_suppkey").isin([row.s_suppkey for row in supplier_complaints.collect()]))
        )
        .groupBy("p_brand", "p_type", "p_size")
        .agg(expr("count(distinct ps_suppkey)").alias("supplier_cnt"))
        .orderBy(col("supplier_cnt").desc(), "p_brand", "p_type", "p_size")
    )
    
    return result

result16 = query_16(spark, dataset_path)
#result16.show()

def query_17(spark, dataset_path):
    var_1 = "Brand#23"
    var_2 = "MED BOX"

    # Read data
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    part_df = spark.read.parquet(f"{dataset_path}/part.parquet")

    # Filtrar partes pela marca e container
    filtered_parts = part_df.filter(
        (col("p_brand") == var_1) & 
        (col("p_container") == var_2)
    )

    # Calcular a média de quantidade por partkey (0.2 * avg)
    avg_quantity = (
        line_item_df.groupBy("l_partkey")
        .agg(expr("0.2 * avg(l_quantity)").alias("avg_quantity"))
    )

    # Juntar e filtrar
    result = (
        filtered_parts.join(line_item_df, filtered_parts.p_partkey == line_item_df.l_partkey)
        .join(avg_quantity, filtered_parts.p_partkey == avg_quantity.l_partkey)
        .filter(col("l_quantity") < col("avg_quantity"))
        .select(expr("sum(l_extendedprice) / 7.0").alias("avg_yearly"))
    )
    
    return result

result17 = query_17(spark, dataset_path)
#result17.show()

def query_18(spark, dataset_path):
    var_1 = 300

    # Read data with distinct aliases
    customer_df = spark.read.parquet(f"{dataset_path}/customer.parquet").alias("cust")
    lineitem_df1 = spark.read.parquet(f"{dataset_path}/lineitem.parquet").alias("li1")  # Para subquery
    lineitem_df2 = spark.read.parquet(f"{dataset_path}/lineitem.parquet").alias("li2")  # Para join principal
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet").alias("ord")

    # Subquery: Find orders with total quantity > 300 (usando li1)
    large_orders = (
        lineitem_df1.groupBy(col("li1.l_orderkey"))
        .agg(spark_sum(col("li1.l_quantity")).alias("total_quantity"))
        .filter(col("total_quantity") > var_1)
        .select(col("li1.l_orderkey"))
    )

    # Main query: Join and aggregate (usando li2)
    result = (
        orders_df.join(large_orders, col("ord.o_orderkey") == col("l_orderkey"))
        .join(customer_df, col("ord.o_custkey") == col("cust.c_custkey"))
        .join(lineitem_df2, col("ord.o_orderkey") == col("li2.l_orderkey"))  # Usando li2 aqui
        .groupBy(
            col("cust.c_name"),
            col("cust.c_custkey"),
            col("ord.o_orderkey"),
            col("ord.o_orderdate"),
            col("ord.o_totalprice")
        )
        .agg(spark_sum(col("li2.l_quantity")).alias("total_quantity"))  # Usando li2 aqui
        .select(
            col("cust.c_name"),
            col("cust.c_custkey"),
            col("ord.o_orderkey"),
            col("ord.o_orderdate"),
            col("ord.o_totalprice"),
            col("total_quantity")
        )
        .orderBy(col("ord.o_totalprice").desc(), col("ord.o_orderdate").desc())
        .limit(100)
    )

    return result

result18 = query_18(spark, dataset_path)
#result18.show()

def query_19(spark, dataset_path):
    # Read data
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    part_df = spark.read.parquet(f"{dataset_path}/part.parquet")

    result = (
        part_df.join(line_item_df, part_df.p_partkey == line_item_df.l_partkey)
        .filter(
            (col("l_shipmode").isin(["AIR", "AIR REG"])) &
            (col("l_shipinstruct") == "DELIVER IN PERSON") &
            (
                ((col("p_brand") == "Brand#12") &
                 (col("p_container").isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])) &
                 (col("l_quantity") >= 1) & (col("l_quantity") <= 11) &
                 (col("p_size") >= 1) & (col("p_size") <= 5)) |
                ((col("p_brand") == "Brand#23") &
                 (col("p_container").isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])) &
                 (col("l_quantity") >= 10) & (col("l_quantity") <= 20) &
                 (col("p_size") >= 1) & (col("p_size") <= 10)) |
                ((col("p_brand") == "Brand#34") &
                 (col("p_container").isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])) &
                 (col("l_quantity") >= 20) & (col("l_quantity") <= 30) &
                 (col("p_size") >= 1) & (col("p_size") <= 15))
            )
        )
        .select(
            expr("sum(l_extendedprice * (1 - l_discount))").alias("revenue")
        )
    )
    
    return result

result19 = query_19(spark, dataset_path)
#result19.show()

def query_20(spark, dataset_path):
    var_1 = date(1994, 1, 1)
    var_2 = date(1995, 1, 1)
    var_3 = "CANADA"
    var_4 = "forest"

    # Read data
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    nation_df = spark.read.parquet(f"{dataset_path}/nation.parquet")
    supplier_df = spark.read.parquet(f"{dataset_path}/supplier.parquet")
    part_df = spark.read.parquet(f"{dataset_path}/part.parquet")
    part_supp_df = spark.read.parquet(f"{dataset_path}/partsupp.parquet")

    # Calcular sum_quantity (0.5 * sum(l_quantity))
    res_1 = (
        line_item_df.filter(
            (col("l_shipdate") >= var_1) &
            (col("l_shipdate") < var_2)
        )
        .groupBy("l_partkey", "l_suppkey")
        .agg(expr("0.5 * sum(l_quantity)").alias("sum_quantity"))
    )

    # Filtrar nation
    res_2 = nation_df.filter(col("n_name") == var_3)
    res_3 = supplier_df.join(res_2, supplier_df.s_nationkey == res_2.n_nationkey)

    # Filtrar parts
    filtered_parts = part_df.filter(col("p_name").startswith(var_4))

    result = (
        part_supp_df.join(filtered_parts, part_supp_df.ps_partkey == filtered_parts.p_partkey)
        .join(res_1, 
              (part_supp_df.ps_suppkey == res_1.l_suppkey) & 
              (part_supp_df.ps_partkey == res_1.l_partkey))
        .filter(col("ps_availqty") > col("sum_quantity"))
        .join(res_3, part_supp_df.ps_suppkey == res_3.s_suppkey)
        .select("s_name", "s_address")
        .orderBy("s_name")
    )
    
    return result

result20 = query_20(spark, dataset_path)
#result20.show()

def query_21(spark, dataset_path):
    var_1 = "SAUDI ARABIA"

    # Read data with aliases
    line_item_df = spark.read.parquet(f"{dataset_path}/lineitem.parquet")
    supplier_df = spark.read.parquet(f"{dataset_path}/supplier.parquet")
    nation_df = spark.read.parquet(f"{dataset_path}/nation.parquet")
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")

    # Aliases para as diferentes instâncias
    l1 = line_item_df.alias("l1")
    l2 = line_item_df.alias("l2")
    l3 = line_item_df.alias("l3")

    # EXISTS: outro supplier no mesmo order
    exists_condition = (
        l1.join(l2, 
               (col("l1.l_orderkey") == col("l2.l_orderkey")) &
               (col("l1.l_suppkey") != col("l2.l_suppkey")),
               "leftsemi")
    )

    # NOT EXISTS: outro supplier com atraso no mesmo order  
    not_exists_condition = (
        exists_condition.join(l3,
                            (col("l1.l_orderkey") == col("l3.l_orderkey")) &
                            (col("l1.l_suppkey") != col("l3.l_suppkey")) &
                            (col("l3.l_receiptdate") > col("l3.l_commitdate")),
                            "leftanti")
    )

    result = (
        not_exists_condition.join(supplier_df, col("l1.l_suppkey") == supplier_df.s_suppkey)
        .join(orders_df, col("l1.l_orderkey") == orders_df.o_orderkey)
        .join(nation_df, supplier_df.s_nationkey == nation_df.n_nationkey)
        .filter(
            (col("o_orderstatus") == "F") &
            (col("l1.l_receiptdate") > col("l1.l_commitdate")) &
            (col("n_name") == var_1)
        )
        .groupBy("s_name")
        .agg(count("*").alias("numwait"))
        .orderBy(col("numwait").desc(), col("s_name"))
        .limit(100)
    )
    
    return result

result21 = query_21(spark, dataset_path)
#result21.show()

def query_22(spark, dataset_path):
    # Códigos de país especificados
    country_codes = ["13", "31", "23", "29", "30", "18", "17"]

    # Read data
    orders_df = spark.read.parquet(f"{dataset_path}/orders.parquet")
    customer_df = spark.read.parquet(f"{dataset_path}/customer.parquet")

    # Subquery: calcular média de saldo para códigos de país específicos
    avg_bal = (
        customer_df.filter(
            (col("c_acctbal") > 0.0) &
            (substring(col("c_phone"), 1, 2).isin(country_codes))
        )
        .select(expr("avg(c_acctbal)").alias("avg_acctbal"))
        .first()["avg_acctbal"]
    )

    # Customers sem orders (NOT EXISTS) - USANDO LEFT ANTI
    customers_no_orders = customer_df.join(
        orders_df.select("o_custkey"), 
        customer_df.c_custkey == orders_df.o_custkey, 
        "left_anti"
    )

    # Query principal
    result = (
        customers_no_orders.filter(
            (substring(col("c_phone"), 1, 2).isin(country_codes)) &
            (col("c_acctbal") > avg_bal)
        )
        .withColumn("cntrycode", substring(col("c_phone"), 1, 2))
        .groupBy("cntrycode")
        .agg(
            count("*").alias("numcust"),
            expr("sum(c_acctbal)").alias("totacctbal")
        )
        .orderBy("cntrycode")
    )
    
    return result

result22 = query_22(spark, dataset_path)
#result22.show()

spark.stop()
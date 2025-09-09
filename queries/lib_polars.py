import polars as pl
import os
from datetime import date

SCALE = os.environ.get("SCALE_FACTOR", "1")
dataset_path = f"data_tbl/scale-{SCALE}/"

def query_01(dataset_path, SCALE):
    VAR1 = date(1998, 9, 2)
    
    lineitem_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    
    result = (
        lineitem_ds.filter(pl.col("l_shipdate") <= VAR1)
        .with_columns(
            disc_price=pl.col("l_extendedprice") * (1 - pl.col("l_discount")),
            charge=pl.col("l_extendedprice") * (1 - pl.col("l_discount")) * (1 + pl.col("l_tax"))
        )
        .group_by(["l_returnflag", "l_linestatus"])
        .agg(
            sum_qty=pl.col("l_quantity").sum(),
            sum_base_price=pl.col("l_extendedprice").sum(),
            sum_disc_price=pl.col("disc_price").sum(),
            sum_charge=pl.col("charge").sum(),
            avg_qty=pl.col("l_quantity").mean(),
            avg_price=pl.col("l_extendedprice").mean(),
            avg_disc=pl.col("l_discount").mean(),
            count_order=pl.col("l_orderkey").count()
        )
        .sort(["l_returnflag", "l_linestatus"])
    )
    
    return result

def query_02(dataset_path):
    var_1 = 15
    var_2 = "BRASS"
    var_3 = "EUROPE"

    region_ds = pl.read_parquet(os.path.join(dataset_path, "region.parquet"))
    nation_ds = pl.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    part_ds = pl.read_parquet(os.path.join(dataset_path, "part.parquet"))
    part_supp_ds = pl.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))

    # Subquery para custo mínimo
    min_cost_subquery = (
        part_supp_ds.join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("r_name") == var_3)
        .group_by("ps_partkey")
        .agg(min_cost=pl.col("ps_supplycost").min())
    )

    # Query principal
    result = (
        part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
        .filter(
            (pl.col("p_size") == var_1) &
            (pl.col("p_type").str.ends_with(var_2)) &
            (pl.col("r_name") == var_3)
        )
        .join(min_cost_subquery, left_on=["p_partkey", "ps_supplycost"], right_on=["ps_partkey", "min_cost"])
        .select([
            "s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr",
            "s_address", "s_phone", "s_comment"
        ])
        .with_columns(pl.all().exclude(["s_acctbal", "p_partkey"]).str.strip_chars())
        .sort(["s_acctbal", "n_name", "s_name", "p_partkey"], descending=[True, False, False, False])
        .head(100)
    )
    
    return result

def query_03(dataset_path):
    var_1 = var_2 = date(1995, 3, 15)
    var_3 = "BUILDING"

    customer_ds = pl.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    result = (
        customer_ds.filter(pl.col("c_mktsegment") == var_3)
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .filter(
            (pl.col("o_orderdate") < var_2) &
            (pl.col("l_shipdate") > var_1)
        )
        .with_columns(
            revenue=pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
        )
        .group_by(["o_orderkey", "o_orderdate", "o_shippriority"])
        .agg(revenue=pl.col("revenue").sum())
        .select([
            pl.col("o_orderkey").alias("l_orderkey"),
            "revenue",
            "o_orderdate",
            "o_shippriority"
        ])
        .sort(["revenue", "o_orderdate"], descending=[True, False])
        .head(10)
    )
    
    return result

def query_04(dataset_path):
    var_1 = date(1993, 10, 1)
    var_2 = date(1994, 1, 1)

    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    result = (
        line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .filter(
            (pl.col("o_orderdate").is_between(var_1, var_2, closed="left")) &
            (pl.col("l_commitdate") < pl.col("l_receiptdate"))
        )
        .unique(subset=["o_orderpriority", "l_orderkey"])
        .group_by("o_orderpriority")
        .agg(order_count=pl.len())
        .with_columns(pl.col("order_count").cast(pl.Int64))
        .sort("o_orderpriority")
    )
    
    return result

def query_05(dataset_path):
    var_1 = "ASIA"
    var_2 = date(1994, 1, 1)
    var_3 = date(1995, 1, 1)

    region_ds = pl.read_parquet(os.path.join(dataset_path, "region.parquet"))
    nation_ds = pl.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    customer_ds = pl.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))

    result = (
        region_ds.join(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
        .join(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(supplier_ds, left_on=["l_suppkey", "n_nationkey"], right_on=["s_suppkey", "s_nationkey"])
        .filter(
            (pl.col("r_name") == var_1) &
            (pl.col("o_orderdate").is_between(var_2, var_3, closed="left"))
        )
        .with_columns(
            revenue=pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
        )
        .group_by("n_name")
        .agg(revenue=pl.col("revenue").sum())
        .sort("revenue", descending=True)
    )
    
    return result

def query_06(dataset_path):
    var_1 = date(1994, 1, 1)
    var_2 = date(1995, 1, 1)
    var_3 = 0.05
    var_4 = 0.07
    var_5 = 24

    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))

    result = (
        line_item_ds.filter(
            (pl.col("l_shipdate").is_between(var_1, var_2, closed="left")) &
            (pl.col("l_discount").is_between(var_3, var_4)) &
            (pl.col("l_quantity") < var_5)
        )
        .with_columns(
            revenue=pl.col("l_extendedprice") * pl.col("l_discount")
        )
        .select(pl.col("revenue").sum().round(2).alias("revenue"))
    )
    
    return result

def query_07(dataset_path):
    nation_ds = pl.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    customer_ds = pl.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))

    n1 = nation_ds.filter(pl.col("n_name") == "FRANCE")
    n2 = nation_ds.filter(pl.col("n_name") == "GERMANY")

    var_1 = date(1995, 1, 1)
    var_2 = date(1996, 12, 31)

    # Primeira combinação
    df1 = (
        customer_ds.join(n1, left_on="c_nationkey", right_on="n_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .rename({"n_name": "cust_nation"})
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(n2, left_on="s_nationkey", right_on="n_nationkey")
        .rename({"n_name": "supp_nation"})
    )

    # Segunda combinação
    df2 = (
        customer_ds.join(n2, left_on="c_nationkey", right_on="n_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .rename({"n_name": "cust_nation"})
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(n1, left_on="s_nationkey", right_on="n_nationkey")
        .rename({"n_name": "supp_nation"})
    )

    result = (
        pl.concat([df1, df2])
        .filter(pl.col("l_shipdate").is_between(var_1, var_2))
        .with_columns(
            volume=pl.col("l_extendedprice") * (1 - pl.col("l_discount")),
            l_year=pl.col("l_shipdate").dt.year()
        )
        .group_by(["supp_nation", "cust_nation", "l_year"])
        .agg(revenue=pl.col("volume").sum())
        .sort(["supp_nation", "cust_nation", "l_year"])
    )
    
    return result

def query_08(dataset_path):
    part_ds = pl.read_parquet(os.path.join(dataset_path, "part.parquet"))
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    customer_ds = pl.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    nation_ds = pl.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    region_ds = pl.read_parquet(os.path.join(dataset_path, "region.parquet"))

    n1 = nation_ds.select(["n_nationkey", "n_regionkey"])
    n2 = nation_ds.select(["n_nationkey", "n_name"])

    result = (
        part_ds.join(line_item_ds, left_on="p_partkey", right_on="l_partkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .join(customer_ds, left_on="o_custkey", right_on="c_custkey")
        .join(n1, left_on="c_nationkey", right_on="n_nationkey")
        .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("r_name") == "AMERICA")
        .join(n2, left_on="s_nationkey", right_on="n_nationkey")
        .filter(
            (pl.col("o_orderdate").is_between(date(1995, 1, 1), date(1996, 12, 31))) &
            (pl.col("p_type") == "ECONOMY ANODIZED STEEL")
        )
        .with_columns(
            o_year=pl.col("o_orderdate").dt.year(),
            # Criar a coluna volume primeiro
            volume=(pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
        )
        .with_columns(
            # Agora usar a coluna volume que foi criada
            _tmp=pl.when(pl.col("n_name") == "BRAZIL").then(pl.col("volume")).otherwise(0)
        )
        .group_by("o_year")
        .agg(mkt_share=(pl.col("_tmp").sum() / pl.col("volume").sum()).round(2))
        .sort("o_year")
    )
    
    return result

def query_09(dataset_path):
    part_ds = pl.read_parquet(os.path.join(dataset_path, "part.parquet"))
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    part_supp_ds = pl.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    nation_ds = pl.read_parquet(os.path.join(dataset_path, "nation.parquet"))

    result = (
        line_item_ds.join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(part_supp_ds, left_on=["l_suppkey", "l_partkey"], right_on=["ps_suppkey", "ps_partkey"])
        .join(part_ds, left_on="l_partkey", right_on="p_partkey")
        .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("p_name").str.contains("green"))
        .select([
            pl.col("n_name").alias("nation"),
            pl.col("o_orderdate").dt.year().alias("o_year"),
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")) - pl.col("ps_supplycost") * pl.col("l_quantity")).alias("amount")
        ])
        .group_by(["nation", "o_year"])
        .agg(sum_profit=pl.col("amount").sum().round(2))
        .sort(["nation", "o_year"], descending=[False, True])
    )
    
    return result

def query_10(dataset_path):
    customer_ds = pl.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    nation_ds = pl.read_parquet(os.path.join(dataset_path, "nation.parquet"))

    var_1 = date(1993, 10, 1)
    var_2 = date(1994, 1, 1)

    result = (
        customer_ds.join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
        .filter(
            (pl.col("o_orderdate").is_between(var_1, var_2, closed="left")) &
            (pl.col("l_returnflag") == "R")
        )
        .with_columns(
            revenue=pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
        )
        .group_by([
            "c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"
        ])
        .agg(revenue=pl.col("revenue").sum().round(2))
        .with_columns([
            pl.col("c_address").str.strip_chars(),
            pl.col("c_comment").str.strip_chars()
        ])
        .select([
            "c_custkey", "c_name", "revenue", "c_acctbal", "n_name", 
            "c_address", "c_phone", "c_comment"
        ])
        .sort("revenue", descending=True)
        .head(20)
    )
    
    return result

def query_11(dataset_path, scale):
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    part_supp_ds = pl.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))
    nation_ds = pl.read_parquet(os.path.join(dataset_path, "nation.parquet"))

    var_1 = "GERMANY"
    var_2 = 0.0001 / float(scale)

    # Primeira parte: join das tabelas
    res_1 = (
        part_supp_ds.join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("n_name") == var_1)
    )

    # Calcular valor total
    total_value = (
        res_1.select((pl.col("ps_supplycost") * pl.col("ps_availqty")).sum().round(2) * var_2)
        .item()
    )

    # Agrupamento e filtro
    result = (
        res_1.group_by("ps_partkey")
        .agg(value=(pl.col("ps_supplycost") * pl.col("ps_availqty")).sum().round(2))
        .filter(pl.col("value") > total_value)
        .select(["ps_partkey", "value"])
        .sort("value", descending=True)
    )
    
    return result

def query_12(dataset_path):
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    var_1 = "MAIL"
    var_2 = "SHIP"
    var_3 = date(1994, 1, 1)
    var_4 = date(1995, 1, 1)

    result = (
        orders_ds.join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("l_shipmode").is_in([var_1, var_2]))
        .filter(
            (pl.col("l_commitdate") < pl.col("l_receiptdate")) &
            (pl.col("l_shipdate") < pl.col("l_commitdate")) &
            (pl.col("l_receiptdate").is_between(var_3, var_4, closed="left"))
        )
        .with_columns(
            high_line_count=pl.when(pl.col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"])).then(1).otherwise(0),
            low_line_count=pl.when(~pl.col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"])).then(1).otherwise(0)
        )
        .group_by("l_shipmode")
        .agg([
            pl.col("high_line_count").sum(),
            pl.col("low_line_count").sum()
        ])
        .sort("l_shipmode")
    )
    
    return result

def query_13(dataset_path):
    var_1 = "special"
    var_2 = "requests"

    customer_ds = pl.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    result = (
        customer_ds.join(
            orders_ds.filter(~pl.col("o_comment").str.contains(f"{var_1}.*{var_2}")),
            left_on="c_custkey", right_on="o_custkey", how="left"
        )
        .group_by("c_custkey")
        .agg(c_count=pl.col("o_orderkey").count())
        .group_by("c_count")
        .agg(custdist=pl.len())
        .select(["c_count", "custdist"])
        .sort(["custdist", "c_count"], descending=[True, True])
    )
    
    return result

def query_14(dataset_path):
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    part_ds = pl.read_parquet(os.path.join(dataset_path, "part.parquet"))

    var_1 = date(1995, 9, 1)
    var_2 = date(1995, 10, 1)

    result = (
        line_item_ds.join(part_ds, left_on="l_partkey", right_on="p_partkey")
        .filter(pl.col("l_shipdate").is_between(var_1, var_2, closed="left"))
        .with_columns(
            discounted_price=pl.col("l_extendedprice") * (1 - pl.col("l_discount")),
            promo_revenue=pl.when(pl.col("p_type").str.starts_with("PROMO"))
                          .then(pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                          .otherwise(0)
        )
        .select(
            (100.00 * pl.col("promo_revenue").sum() / pl.col("discounted_price").sum()).round(2).alias("promo_revenue")
        )
    )
    
    return result

def query_15(dataset_path):
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))

    var_1 = date(1996, 1, 1)
    var_2 = date(1996, 4, 1)

    # Criar a view revenue
    revenue_ds = (
        line_item_ds.filter(pl.col("l_shipdate").is_between(var_1, var_2, closed="left"))
        .with_columns(total_revenue=pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
        .group_by("l_suppkey")
        .agg(total_revenue=pl.col("total_revenue").sum())
        .rename({"l_suppkey": "supplier_no"})
    )

    # Encontrar o máximo total_revenue
    max_revenue = revenue_ds["total_revenue"].max()

    # Juntar com supplier e filtrar
    result = (
        supplier_ds.join(revenue_ds, left_on="s_suppkey", right_on="supplier_no")
        .filter(pl.col("total_revenue") == max_revenue)
        .with_columns(total_revenue=pl.col("total_revenue").round(2))
        .select(["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"])
        .sort("s_suppkey")
    )
    
    return result

def query_16(dataset_path):
    part_supp_ds = pl.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))
    part_ds = pl.read_parquet(os.path.join(dataset_path, "part.parquet"))
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))

    var_1 = "Brand#45"
    sizes = [49, 14, 23, 45, 19, 3, 36, 9]

    # Filtrar suppliers com complaints
    supplier_complaints = (
        supplier_ds.filter(pl.col("s_comment").str.contains("Customer.*Complaints"))
        .select("s_suppkey")
    )

    result = (
        part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .filter(
            (pl.col("p_brand") != var_1) &
            (~pl.col("p_type").str.starts_with("MEDIUM POLISHED")) &
            (pl.col("p_size").is_in(sizes)) &
            (~pl.col("ps_suppkey").is_in(supplier_complaints["s_suppkey"]))
        )
        .group_by(["p_brand", "p_type", "p_size"])
        .agg(supplier_cnt=pl.col("ps_suppkey").n_unique())
        .sort(["supplier_cnt", "p_brand", "p_type", "p_size"], descending=[True, False, False, False])
    )
    
    return result

def query_17(dataset_path):
    var_1 = "Brand#23"
    var_2 = "MED BOX"

    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    part_ds = pl.read_parquet(os.path.join(dataset_path, "part.parquet"))

    # Filtrar partes pela marca e container
    filtered_parts = part_ds.filter(
        (pl.col("p_brand") == var_1) & (pl.col("p_container") == var_2)
    )

    # Calcular a média de quantidade por partkey (0.2 * avg)
    avg_quantity = (
        line_item_ds.group_by("l_partkey")
        .agg(avg_qty=pl.col("l_quantity").mean() * 0.2)
    )

    # Juntar e filtrar
    result = (
        filtered_parts.join(line_item_ds, left_on="p_partkey", right_on="l_partkey")
        .join(avg_quantity, left_on="p_partkey", right_on="l_partkey")
        .filter(pl.col("l_quantity") < pl.col("avg_qty"))
        .select((pl.col("l_extendedprice").sum() / 7.0).round(2).alias("avg_yearly"))
    )
    
    return result

def query_18(dataset_path):
    customer_ds = pl.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    var_1 = 300

    # Encontrar orders com quantidade total > 300 (usando implode)
    large_orders = (
        line_item_ds.group_by("l_orderkey")
        .agg(sum_quantity=pl.col("l_quantity").sum())
        .filter(pl.col("sum_quantity") > var_1)
        .select(pl.col("l_orderkey").implode())
    )

    # Corrigir o join - usar chaves corretas
    result = (
        orders_ds.filter(pl.col("o_orderkey").is_in(large_orders["l_orderkey"].explode()))
        .join(customer_ds, left_on="o_custkey", right_on="c_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .group_by(["c_name", "o_custkey", "o_orderkey", "o_orderdate", "o_totalprice"])
        .agg(col6=pl.col("l_quantity").sum())
        .select([
            "c_name", 
            pl.col("o_custkey").alias("c_custkey"), 
            "o_orderkey", 
            "o_orderdate", 
            "o_totalprice", 
            "col6"
        ])
        .sort(["o_totalprice", "o_orderdate"], descending=[True, False])
        .head(100)
    )
    
    return result

def query_19(dataset_path):
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    part_ds = pl.read_parquet(os.path.join(dataset_path, "part.parquet"))

    result = (
        part_ds.join(line_item_ds, left_on="p_partkey", right_on="l_partkey")
        .filter(
            (pl.col("l_shipmode").is_in(["AIR", "AIR REG"])) &
            (pl.col("l_shipinstruct") == "DELIVER IN PERSON") &
            (
                ((pl.col("p_brand") == "Brand#12") &
                 (pl.col("p_container").is_in(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])) &
                 (pl.col("l_quantity").is_between(1, 11)) &
                 (pl.col("p_size").is_between(1, 5)))
                |
                ((pl.col("p_brand") == "Brand#23") &
                 (pl.col("p_container").is_in(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])) &
                 (pl.col("l_quantity").is_between(10, 20)) &
                 (pl.col("p_size").is_between(1, 10)))
                |
                ((pl.col("p_brand") == "Brand#34") &
                 (pl.col("p_container").is_in(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])) &
                 (pl.col("l_quantity").is_between(20, 30)) &
                 (pl.col("p_size").is_between(1, 15)))
            )
        )
        .select(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum().round(2).alias("revenue")
        )
    )
    
    return result

def query_20(dataset_path):
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    nation_ds = pl.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    part_ds = pl.read_parquet(os.path.join(dataset_path, "part.parquet"))
    part_supp_ds = pl.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))

    var_1 = date(1994, 1, 1)
    var_2 = date(1995, 1, 1)
    var_3 = "CANADA"
    var_4 = "forest"

    # Calcular sum_quantity
    res_1 = (
        line_item_ds.filter(pl.col("l_shipdate").is_between(var_1, var_2, closed="left"))
        .group_by(["l_partkey", "l_suppkey"])
        .agg(sum_quantity=pl.col("l_quantity").sum() * 0.5)
    )

    # Filtrar nation
    res_2 = nation_ds.filter(pl.col("n_name") == var_3)
    res_3 = supplier_ds.join(res_2, left_on="s_nationkey", right_on="n_nationkey")

    # Filtrar parts
    filtered_parts = part_ds.filter(pl.col("p_name").str.starts_with(var_4))

    result = (
        part_supp_ds.join(filtered_parts, left_on="ps_partkey", right_on="p_partkey")
        .join(res_1, left_on=["ps_suppkey", "ps_partkey"], right_on=["l_suppkey", "l_partkey"])
        .filter(pl.col("ps_availqty") > pl.col("sum_quantity"))
        .join(res_3, left_on="ps_suppkey", right_on="s_suppkey")
        .with_columns(pl.col("s_address").str.strip_chars())
        .select(["s_name", "s_address"])
        .sort("s_name")
    )
    
    return result

def query_21(dataset_path):
    line_item_ds = pl.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    supplier_ds = pl.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    nation_ds = pl.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    var_1 = "SAUDI ARABIA"

    # Lineitems com recebimento após commit
    late_items = line_item_ds.filter(pl.col("l_receiptdate") > pl.col("l_commitdate"))

    # Orders com mais de 1 supplier
    multi_supplier_orders = (
        line_item_ds.group_by("l_orderkey")
        .agg(n_suppliers=pl.col("l_suppkey").n_unique())
        .filter(pl.col("n_suppliers") > 1)
    )

    # Combinar as condições
    result = (
        late_items.join(multi_supplier_orders, on="l_orderkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .filter(
            (pl.col("n_name") == var_1) &
            (pl.col("o_orderstatus") == "F")
        )
        .group_by("s_name")
        .agg(numwait=pl.len())
        .sort(["numwait", "s_name"], descending=[True, False])
        .head(100)
    )
    
    return result

def query_22(dataset_path):
    orders_ds = pl.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    customer_ds = pl.read_parquet(os.path.join(dataset_path, "customer.parquet"))

    # Extrair código do país e filtrar
    res_1 = (
        customer_ds.with_columns(cntrycode=pl.col("c_phone").str.slice(0, 2))
        .filter(pl.col("cntrycode").is_in(["13", "31", "23", "29", "30", "18", "17"]))
        .select(["c_acctbal", "c_custkey", "cntrycode"])
    )

    # Calcular média de saldo
    avg_bal = (
        res_1.filter(pl.col("c_acctbal") > 0.0)
        .select(avg_acctbal=pl.col("c_acctbal").mean())
        .item()
    )

    # Customers sem orders
    customers_with_orders = orders_ds.select("o_custkey").unique()
    customers_no_orders = res_1.filter(~pl.col("c_custkey").is_in(customers_with_orders["o_custkey"]))

    # Filtrar por saldo acima da média
    result = (
        customers_no_orders.filter(pl.col("c_acctbal") > avg_bal)
        .group_by("cntrycode")
        .agg(
            numcust=pl.len(),
            totacctbal=pl.col("c_acctbal").sum().round(2)
        )
        .sort("cntrycode")
    )
    
    return result

# Exemplo de uso
if __name__ == "__main__":
    result1 = query_01(dataset_path, SCALE)
    #print(f"Polars Query 01: {result1}")
    
    result2 = query_02(dataset_path) 
    #print(f"Polars Query 02: {result2}")

    result3 = query_03(dataset_path)
    #print(f"Polars Query 03: {result3}")
    
    result4 = query_04(dataset_path)
    #print(f"Polars Query 04: {result4}")

    result5 = query_05(dataset_path)
    #print(f"Polars Query 05: {result5}")

    result6 = query_06(dataset_path)
    #print(f"Polars Query 06: {result6}")

    result7 = query_07(dataset_path)
    #print(f"Polars Query 07: {result7}")

    result8 = query_08(dataset_path)
    #print(f"Polars Query 08: {result8}")

    result9 = query_09(dataset_path)
    #print(f"Polars Query 09: {result9}")

    result10 = query_10(dataset_path)
    #print(f"Polars Query 10: {result10}")

    result11 = query_11(dataset_path, SCALE)
    #print(f"Polars Query 11: {result11}")

    result12 = query_12(dataset_path)
    #print(f"Polars Query 12: {result12}")

    result13 = query_13(dataset_path)
    #print(f"Polars Query 13: {result13}")

    result14 = query_14(dataset_path)
    #print(f"Polars Query 14: {result14}")

    result15 = query_15(dataset_path)
    #print(f"Polars Query 15: {result15}")

    result16 = query_16(dataset_path)
    #print(f"Polars Query 16: {result16}")

    result17 = query_17(dataset_path)
    #print(f"Polars Query 17: {result17}")

    result18 = query_18(dataset_path)
    #print(f"Polars Query 18: {result18}")

    result19 = query_19(dataset_path)
    #print(f"Polars Query 19: {result19}")

    result20 = query_20(dataset_path)
    #print(f"Polars Query 20: {result20}")

    result21 = query_21(dataset_path)
    #print(f"Polars Query 21: {result21}")

    result22 = query_22(dataset_path)
    #print(f"Polars Query 22: {result22}")
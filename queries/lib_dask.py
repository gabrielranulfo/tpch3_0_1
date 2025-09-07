from datetime import datetime, timedelta
import dask.config as ddconfig
import dask.dataframe as dd
import os

N_CORES = int(os.environ.get("N_CORES", "1"))
SCALE = os.environ.get("SCALE_FACTOR", "1")

ddconfig.set(scheduler='threads', num_workers=N_CORES)
dataset_path = f"data_tbl/scale-{SCALE}/"
fs = None

def query_01(dataset_path, fs, SCALE):
    VAR1 = datetime(1998, 9, 2)
    #lineitem_ds = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    lineitem_ds = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)


    lineitem_filtered = lineitem_ds[lineitem_ds.l_shipdate <= VAR1]
    lineitem_filtered["sum_qty"] = lineitem_filtered.l_quantity
    lineitem_filtered["sum_base_price"] = lineitem_filtered.l_extendedprice
    lineitem_filtered["avg_qty"] = lineitem_filtered.l_quantity
    lineitem_filtered["avg_price"] = lineitem_filtered.l_extendedprice
    lineitem_filtered["sum_disc_price"] = lineitem_filtered.l_extendedprice * (
        1 - lineitem_filtered.l_discount
    )
    lineitem_filtered["sum_charge"] = (
        lineitem_filtered.l_extendedprice
        * (1 - lineitem_filtered.l_discount)
        * (1 + lineitem_filtered.l_tax)
    )
    lineitem_filtered["avg_disc"] = lineitem_filtered.l_discount
    lineitem_filtered["count_order"] = lineitem_filtered.l_orderkey
    gb = lineitem_filtered.groupby(["l_returnflag", "l_linestatus"])

    total = gb.agg(
        {
            "sum_qty": "sum",
            "sum_base_price": "sum",
            "sum_disc_price": "sum",
            "sum_charge": "sum",
            "avg_qty": "mean",
            "avg_price": "mean",
            "avg_disc": "mean",
            "count_order": "size",
        }
    )

    return total.reset_index().sort_values(["l_returnflag", "l_linestatus"])

result = query_01(dataset_path, fs, SCALE)
# print(result.compute())

def query_02(dataset_path, fs, SCALE):
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    region_ds = dd.read_parquet(os.path.join(dataset_path, "region.parquet"), filesystem=fs)
    nation_filtered = dd.read_parquet(os.path.join(dataset_path, "nation.parquet"), filesystem=fs)
    supplier_filtered = dd.read_parquet(os.path.join(dataset_path, "supplier.parquet"), filesystem=fs)
    part_filtered = dd.read_parquet(os.path.join(dataset_path, "part.parquet"), filesystem=fs)
    partsupp_filtered = dd.read_parquet(os.path.join(dataset_path, "partsupp.parquet"), filesystem=fs)

    region_filtered = region_ds[(region_ds["r_name"] == var3)]
    r_n_merged = nation_filtered.merge(
        region_filtered, left_on="n_regionkey", right_on="r_regionkey", how="inner"
    )
    s_r_n_merged = r_n_merged.merge(
        supplier_filtered,
        left_on="n_nationkey",
        right_on="s_nationkey",
        how="inner",
    )
    ps_s_r_n_merged = s_r_n_merged.merge(
        partsupp_filtered, left_on="s_suppkey", right_on="ps_suppkey", how="inner"
    )
    part_filtered = part_filtered[
        (part_filtered["p_size"] == var1) & (part_filtered["p_type"].str.endswith(var2))
    ]
    merged_df = part_filtered.merge(
        ps_s_r_n_merged, left_on="p_partkey", right_on="ps_partkey", how="inner"
    )

    # menor custo de supply por partkey
    min_values = merged_df.groupby("p_partkey")["ps_supplycost"].min().reset_index()
    min_values.columns = ["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]

    merged_df = merged_df.merge(
        min_values,
        left_on=["p_partkey", "ps_supplycost"],
        right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"],
        how="inner",
    )

    return (
        merged_df[
            [
                "s_acctbal",
                "s_name",
                "n_name",
                "p_partkey",
                "p_mfgr",
                "s_address",
                "s_phone",
                "s_comment",
            ]
        ]
        .sort_values(
            by=["s_acctbal", "n_name", "s_name", "p_partkey"],
            ascending=[False, True, True, True],
        )
        .head(100, compute=False)  # usa Dask .head() sem computar ainda
    )

result = query_02(dataset_path, fs, SCALE)
# print(result.compute())

def query_03(dataset_path, fs, SCALE):
    var1 = datetime.strptime("1995-03-15", "%Y-%m-%d")
    var2 = "BUILDING"

    lineitem_ds = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    orders_ds = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs)
    customer_ds = dd.read_parquet(os.path.join(dataset_path, "customer.parquet"), filesystem=fs)

    # filtros
    lsel = lineitem_ds.l_shipdate > var1
    osel = orders_ds.o_orderdate < var1
    csel = customer_ds.c_mktsegment == var2

    flineitem = lineitem_ds[lsel]
    forders = orders_ds[osel]
    fcustomer = customer_ds[csel]

    # joins
    jn1 = fcustomer.merge(forders, left_on="c_custkey", right_on="o_custkey")
    jn2 = jn1.merge(flineitem, left_on="o_orderkey", right_on="l_orderkey")

    # cálculo da receita
    jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)

    total = jn2.groupby(["l_orderkey", "o_orderdate", "o_shippriority"])["revenue"].sum()

    return (
        total.reset_index()
        .sort_values(["revenue"], ascending=False)
        .head(10, compute=False)[["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]]
    )

result = query_03(dataset_path, fs, SCALE)
# print(result.compute())

def query_04(dataset_path, fs, SCALE):
    date1 = datetime.strptime("1993-10-01", "%Y-%m-%d")
    date2 = datetime.strptime("1993-07-01", "%Y-%m-%d")

    line_item_ds = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    orders_ds = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs)

    # filtros
    lsel = line_item_ds.l_commitdate < line_item_ds.l_receiptdate
    osel = (orders_ds.o_orderdate < date1) & (orders_ds.o_orderdate >= date2)

    flineitem = line_item_ds[lsel]
    forders = orders_ds[osel]

    # join semi (mantém apenas pedidos que aparecem no lineitem)
    jn = forders.merge(
        flineitem, how="leftsemi", left_on="o_orderkey", right_on="l_orderkey"
    )

    # agregação
    result_df = (
        jn.groupby("o_orderpriority")
        .size()
        .to_frame("order_count")
        .reset_index()
        .sort_values(["o_orderpriority"])
    )

    return result_df

result = query_04(dataset_path, fs, SCALE)
# print(result.compute())

def query_05(dataset_path, fs, SCALE):
    date1 = datetime.strptime("1994-01-01", "%Y-%m-%d")
    date2 = datetime.strptime("1995-01-01", "%Y-%m-%d")

    region_ds = dd.read_parquet(os.path.join(dataset_path, "region.parquet"), filesystem=fs)
    nation_ds = dd.read_parquet(os.path.join(dataset_path, "nation.parquet"), filesystem=fs)
    customer_ds = dd.read_parquet(os.path.join(dataset_path, "customer.parquet"), filesystem=fs)
    line_item_ds = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    orders_ds = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs)
    supplier_ds = dd.read_parquet(os.path.join(dataset_path, "supplier.parquet"), filesystem=fs)

    # filtros
    rsel = region_ds.r_name == "ASIA"
    osel = (orders_ds.o_orderdate >= date1) & (orders_ds.o_orderdate < date2)
    forders = orders_ds[osel]
    fregion = region_ds[rsel]

    # joins
    jn1 = fregion.merge(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
    jn2 = jn1.merge(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
    jn3 = jn2.merge(forders, left_on="c_custkey", right_on="o_custkey")
    jn4 = jn3.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
    jn5 = supplier_ds.merge(
        jn4,
        left_on=["s_suppkey", "s_nationkey"],
        right_on=["l_suppkey", "n_nationkey"],
    )

    # cálculo de receita
    jn5["revenue"] = jn5.l_extendedprice * (1.0 - jn5.l_discount)

    # agregação
    gb = jn5.groupby("n_name")["revenue"].sum()

    return gb.reset_index().sort_values("revenue", ascending=False)

result = query_05(dataset_path, fs, SCALE)
# print(result.compute())

def query_06(dataset_path, fs, SCALE):
    date1 = datetime.strptime("1994-01-01", "%Y-%m-%d")
    date2 = datetime.strptime("1995-01-01", "%Y-%m-%d")
    var3 = 24

    line_item_ds = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)

    # filtro
    sel = (
        (line_item_ds.l_shipdate >= date1)
        & (line_item_ds.l_shipdate < date2)
        & (line_item_ds.l_discount >= 0.05)
        & (line_item_ds.l_discount <= 0.07)
        & (line_item_ds.l_quantity < var3)
    )

    flineitem = line_item_ds[sel]

    # cálculo da receita
    revenue = (flineitem.l_extendedprice * flineitem.l_discount).to_frame()
    return revenue.sum().to_frame("revenue")

result = query_06(dataset_path, fs, SCALE)
# print(result.compute())

def query_07(dataset_path, fs, SCALE):
    var1 = datetime.strptime("1995-01-01", "%Y-%m-%d")
    var2 = datetime.strptime("1997-01-01", "%Y-%m-%d")

    nation_ds = dd.read_parquet(os.path.join(dataset_path, "nation.parquet"), filesystem=fs)
    customer_ds = dd.read_parquet(os.path.join(dataset_path, "customer.parquet"), filesystem=fs)
    line_item_ds = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    orders_ds = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs)
    supplier_ds = dd.read_parquet(os.path.join(dataset_path, "supplier.parquet"), filesystem=fs)

    # filtrar lineitem
    lineitem_filtered = line_item_ds[
        (line_item_ds["l_shipdate"] >= var1) & (line_item_ds["l_shipdate"] < var2)
    ]
    lineitem_filtered["l_year"] = lineitem_filtered["l_shipdate"].dt.year
    lineitem_filtered["revenue"] = lineitem_filtered["l_extendedprice"] * (1.0 - lineitem_filtered["l_discount"])

    # filtrar por nações
    n1 = nation_ds[nation_ds["n_name"] == "FRANCE"]
    n2 = nation_ds[nation_ds["n_name"] == "GERMANY"]

    # ----- do nation 1 -----
    N1_C = customer_ds.merge(n1, left_on="c_nationkey", right_on="n_nationkey", how="inner")
    N1_C = N1_C.drop(columns=["c_nationkey", "n_nationkey"]).rename(columns={"n_name": "cust_nation"})
    N1_C_O = N1_C.merge(orders_ds, left_on="c_custkey", right_on="o_custkey", how="inner")
    N1_C_O = N1_C_O.drop(columns=["c_custkey", "o_custkey"])

    N2_S = supplier_ds.merge(n2, left_on="s_nationkey", right_on="n_nationkey", how="inner")
    N2_S = N2_S.drop(columns=["s_nationkey", "n_nationkey"]).rename(columns={"n_name": "supp_nation"})
    N2_S_L = N2_S.merge(lineitem_filtered, left_on="s_suppkey", right_on="l_suppkey", how="inner")
    N2_S_L = N2_S_L.drop(columns=["s_suppkey", "l_suppkey"])

    total1 = N1_C_O.merge(N2_S_L, left_on="o_orderkey", right_on="l_orderkey", how="inner")
    total1 = total1.drop(columns=["o_orderkey", "l_orderkey"])

    # ----- do nation 2 -----
    N2_C = customer_ds.merge(n2, left_on="c_nationkey", right_on="n_nationkey", how="inner")
    N2_C = N2_C.drop(columns=["c_nationkey", "n_nationkey"]).rename(columns={"n_name": "cust_nation"})
    N2_C_O = N2_C.merge(orders_ds, left_on="c_custkey", right_on="o_custkey", how="inner")
    N2_C_O = N2_C_O.drop(columns=["c_custkey", "o_custkey"])

    N1_S = supplier_ds.merge(n1, left_on="s_nationkey", right_on="n_nationkey", how="inner")
    N1_S = N1_S.drop(columns=["s_nationkey", "n_nationkey"]).rename(columns={"n_name": "supp_nation"})
    N1_S_L = N1_S.merge(lineitem_filtered, left_on="s_suppkey", right_on="l_suppkey", how="inner")
    N1_S_L = N1_S_L.drop(columns=["s_suppkey", "l_suppkey"])

    total2 = N2_C_O.merge(N1_S_L, left_on="o_orderkey", right_on="l_orderkey", how="inner")
    total2 = total2.drop(columns=["o_orderkey", "l_orderkey"])

    # concatenar resultados
    total = dd.concat([total1, total2])
    result_df = (
        total.groupby(["supp_nation", "cust_nation", "l_year"])
        .revenue.agg("sum")
        .reset_index()
    )
    result_df.columns = ["supp_nation", "cust_nation", "l_year", "revenue"]

    return result_df.sort_values(by=["supp_nation", "cust_nation", "l_year"], ascending=True)

result = query_07(dataset_path, fs, SCALE)
# print(result.compute())

def query_08(dataset_path, fs, SCALE):
    var1 = datetime.strptime("1995-01-01", "%Y-%m-%d")
    var2 = datetime.strptime("1997-01-01", "%Y-%m-%d")

    supplier = dd.read_parquet(os.path.join(dataset_path, "supplier.parquet"), filesystem=fs)
    lineitem = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    orders = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs)
    customer = dd.read_parquet(os.path.join(dataset_path, "customer.parquet"), filesystem=fs)
    nation = dd.read_parquet(os.path.join(dataset_path, "nation.parquet"), filesystem=fs)
    region = dd.read_parquet(os.path.join(dataset_path, "region.parquet"), filesystem=fs)
    part = dd.read_parquet(os.path.join(dataset_path, "part.parquet"), filesystem=fs)

    # filtro do part
    part = part[part["p_type"] == "ECONOMY ANODIZED STEEL"][["p_partkey"]]

    # cálculo do volume
    lineitem["volume"] = lineitem["l_extendedprice"] * (1.0 - lineitem["l_discount"])
    total = part.merge(lineitem, left_on="p_partkey", right_on="l_partkey", how="inner")

    total = total.merge(supplier, left_on="l_suppkey", right_on="s_suppkey", how="inner")

    # filtrar orders por datas
    orders = orders[(orders["o_orderdate"] >= var1) & (orders["o_orderdate"] < var2)]
    orders["o_year"] = orders["o_orderdate"].dt.year
    total = total.merge(orders, left_on="l_orderkey", right_on="o_orderkey", how="inner")

    total = total.merge(customer, left_on="o_custkey", right_on="c_custkey", how="inner")

    # merges com nation e region
    n1_filtered = nation[["n_nationkey", "n_regionkey"]]
    total = total.merge(n1_filtered, left_on="c_nationkey", right_on="n_nationkey", how="inner")

    n2_filtered = nation[["n_nationkey", "n_name"]].rename(columns={"n_name": "nation"})
    total = total.merge(n2_filtered, left_on="s_nationkey", right_on="n_nationkey", how="inner")

    region = region[region["r_name"] == "AMERICA"][["r_regionkey"]]
    total = total.merge(region, left_on="n_regionkey", right_on="r_regionkey", how="inner")

    # cálculo de market share
    mkt_brazil = total[total["nation"] == "BRAZIL"].groupby("o_year").volume.sum().reset_index()
    mkt_total = total.groupby("o_year").volume.sum().reset_index()

    final = mkt_total.merge(mkt_brazil, left_on="o_year", right_on="o_year", suffixes=("_mkt", "_brazil"))
    final["mkt_share"] = (final.volume_brazil / final.volume_mkt).round(2)

    return final.sort_values(by=["o_year"], ascending=True)[["o_year", "mkt_share"]]

result = query_08(dataset_path, fs, SCALE)
# print(result.compute())

def query_09(dataset_path, fs, SCALE):
    """
    Query 9: cálculo do lucro por nação e ano para partes com 'green' no nome
    """
    part = dd.read_parquet(os.path.join(dataset_path, "part.parquet"), filesystem=fs)
    partsupp = dd.read_parquet(os.path.join(dataset_path, "partsupp.parquet"), filesystem=fs)
    supplier = dd.read_parquet(os.path.join(dataset_path, "supplier.parquet"), filesystem=fs)
    lineitem = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    orders = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs)
    nation = dd.read_parquet(os.path.join(dataset_path, "nation.parquet"), filesystem=fs)

    # filtrar partes com 'green' no nome
    part = part[part.p_name.str.contains("green")]

    # joins
    subquery = (
        part.merge(partsupp, left_on="p_partkey", right_on="ps_partkey", how="inner")
        .merge(supplier, left_on="ps_suppkey", right_on="s_suppkey", how="inner")
        .merge(
            lineitem,
            left_on=["ps_partkey", "ps_suppkey"],
            right_on=["l_partkey", "l_suppkey"],
            how="inner",
        )
        .merge(orders, left_on="l_orderkey", right_on="o_orderkey", how="inner")
        .merge(nation, left_on="s_nationkey", right_on="n_nationkey", how="inner")
    )

    # cálculo de ano e lucro
    subquery["o_year"] = subquery.o_orderdate.dt.year
    subquery["nation"] = subquery.n_name
    subquery["amount"] = (
        subquery.l_extendedprice * (1 - subquery.l_discount)
        - subquery.ps_supplycost * subquery.l_quantity
    )

    subquery = subquery[["o_year", "nation", "amount"]]

    # agregação final
    result = (
        subquery.groupby(["nation", "o_year"])
        .amount.sum()
        .round(2)
        .reset_index()
        .rename(columns={"amount": "sum_profit"})
        .sort_values(by=["nation", "o_year"], ascending=[True, False])
    )

    return result

result = query_09(dataset_path, fs, SCALE)
# print(result.compute())

def query_10(dataset_path, fs, SCALE):
    """
    Query 10: Top 20 clientes por receita para itens retornados em um intervalo específico.
    """
    customer = dd.read_parquet(os.path.join(dataset_path, "customer.parquet"), filesystem=fs)
    orders = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs)
    lineitem = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    nation = dd.read_parquet(os.path.join(dataset_path, "nation.parquet"), filesystem=fs)

    # intervalo de datas
    orderdate_from = datetime.strptime("1993-10-01", "%Y-%m-%d")
    orderdate_to = datetime.strptime("1994-01-01", "%Y-%m-%d")

    # filtros antes do merge
    orders_filtered = orders[(orders.o_orderdate >= orderdate_from) & (orders.o_orderdate < orderdate_to)]
    lineitem_filtered = lineitem[lineitem.l_returnflag == "R"]

    # merges
    query = (
        lineitem_filtered.merge(orders_filtered, left_on="l_orderkey", right_on="o_orderkey", how="inner")
        .merge(customer, left_on="o_custkey", right_on="c_custkey", how="inner")
        .merge(nation, left_on="c_nationkey", right_on="n_nationkey", how="inner")
    )

    # cálculo de receita
    query["revenue"] = query.l_extendedprice * (1 - query.l_discount)

    # agregação e ordenação
    result = (
        query.groupby(
            [
                "c_custkey",
                "c_name",
                "c_acctbal",
                "c_phone",
                "n_name",
                "c_address",
                "c_comment",
            ]
        )
        .revenue.sum()
        .round(2)
        .reset_index()
        .sort_values(by=["revenue"], ascending=[False])
        .head(20, compute=False)[
            [
                "c_custkey",
                "c_name",
                "revenue",
                "c_acctbal",
                "n_name",
                "c_address",
                "c_phone",
                "c_comment",
            ]
        ]
    )

    return result
result = query_10(dataset_path, fs, SCALE)
#print(f"Query 10 Result: {result.compute()}")

def query_11(dataset_path, fs, SCALE):
    """
    Query 11: Valor total de partsupp por parte para fornecedores da Alemanha,
    filtrando por um threshold baseado no SCALE factor.
    """
    partsupp = dd.read_parquet(os.path.join(dataset_path, "partsupp.parquet"), filesystem=fs)
    supplier = dd.read_parquet(os.path.join(dataset_path, "supplier.parquet"), filesystem=fs)
    nation = dd.read_parquet(os.path.join(dataset_path, "nation.parquet"), filesystem=fs)

    # merges
    joined = partsupp.merge(
        supplier, left_on="ps_suppkey", right_on="s_suppkey", how="inner"
    ).merge(
        nation, left_on="s_nationkey", right_on="n_nationkey", how="inner"
    )

    # filtrar Alemanha
    joined = joined[joined.n_name == "GERMANY"]

    # calcular threshold
    threshold = (joined.ps_supplycost * joined.ps_availqty).sum() * 0.0001 / float(SCALE)

    # calcular valor
    joined["value"] = joined.ps_supplycost * joined.ps_availqty

    # agrupar e filtrar por threshold
    res = joined.groupby("ps_partkey")["value"].sum()
    res = (
        res[res > threshold]
        .round(2)
        .reset_index()
        .sort_values(by="value", ascending=False)
    )

    return res

result = query_11(dataset_path, fs, SCALE)
#print(result.compute())

def query_12(dataset_path, fs, SCALE):
    """
    Query 12: Contagem de linhas de pedidos com prioridade alta e baixa por modo de envio.
    """
    orders = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs)
    lineitem = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)

    # intervalo de datas
    receiptdate_from = datetime.strptime("1994-01-01", "%Y-%m-%d")
    receiptdate_to = receiptdate_from + timedelta(days=365)

    # merge e filtros
    table = orders.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey", how="inner")
    table = table[
        (table.l_shipmode.isin(("MAIL", "SHIP")))
        & (table.l_commitdate < table.l_receiptdate)
        & (table.l_shipdate < table.l_commitdate)
        & (table.l_receiptdate >= receiptdate_from)
        & (table.l_receiptdate < receiptdate_to)
    ]

    # contagem high e low
    mask = table.o_orderpriority.isin(("1-URGENT", "2-HIGH"))
    table["high_line_count"] = 0
    table["high_line_count"] = table.high_line_count.where(~mask, 1)
    table["low_line_count"] = 0
    table["low_line_count"] = table.low_line_count.where(mask, 1)

    # agrupamento e ordenação
    result = (
        table.groupby("l_shipmode")
        .agg({"high_line_count": "sum", "low_line_count": "sum"})
        .reset_index()
        .sort_values(by="l_shipmode")
    )

    return result

result = query_12(dataset_path, fs, SCALE)
#print(result.compute())

def query_13(dataset_path, fs, SCALE):
    """
    Query 13: Distribuição de clientes pelo número de pedidos, ignorando pedidos com 'special requests'.
    """
    customer = dd.read_parquet(os.path.join(dataset_path, "customer.parquet"), filesystem=fs)
    orders = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs)

    # filtrar pedidos que não contenham 'special requests'
    orders_filtered = orders[~orders.o_comment.str.contains("special.*requests")]

    # left join customer com orders
    subquery = customer.merge(
        orders_filtered, left_on="c_custkey", right_on="o_custkey", how="left"
    )

    # contar pedidos por cliente
    subquery = (
        subquery.groupby("c_custkey")
        .o_orderkey.count()
        .to_frame()
        .reset_index()
        .rename(columns={"o_orderkey": "c_count"})
    )

    # agrupar por número de pedidos e contar distribuição
    result = (
        subquery.groupby("c_count")
        .size()
        .to_frame()
        .rename(columns={0: "custdist"})
        .reset_index()
        .sort_values(by=["custdist", "c_count"], ascending=[False, False])
    )

    return result

result = query_13(dataset_path, fs, SCALE)
#print(result.compute())

def query_14(dataset_path, fs, SCALE):
    """
    Query 14: Receita percentual de itens promocionais em um intervalo de datas específico.
    """
    lineitem = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    part = dd.read_parquet(os.path.join(dataset_path, "part.parquet"), filesystem=fs)

    # intervalo de datas
    shipdate_from = datetime.strptime("1995-09-01", "%Y-%m-%d")
    shipdate_to = datetime.strptime("1995-10-01", "%Y-%m-%d")

    # merge e filtro por data
    table = lineitem.merge(part, left_on="l_partkey", right_on="p_partkey", how="inner")
    table = table[(table.l_shipdate >= shipdate_from) & (table.l_shipdate < shipdate_to)]

    # cálculo de receita promocional
    table["line_revenue"] = table.l_extendedprice * (1 - table.l_discount)
    mask = table.p_type.str.startswith("PROMO")
    table["promo_revenue"] = table.line_revenue.where(mask, 0.0)

    # soma total de receita promocional e total
    total_promo_revenue = table.promo_revenue.sum()
    total_revenue = table.line_revenue.sum()

    # calcular percentual
    result = (100.0 * total_promo_revenue / total_revenue).compute()
    
    # transformar em DataFrame
    return dd.from_pandas(
        dd.utils.make_meta({"promo_revenue": float}).from_dict({"promo_revenue": [round(result, 2)]}),
        npartitions=1,
    )

result = query_14(dataset_path, fs, SCALE)
#print(result.compute())

def query_15(dataset_path, fs, SCALE):
    """
    Query 15: Supplier(s) com maior receita em um intervalo específico.
    """
    lineitem = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    supplier = dd.read_parquet(os.path.join(dataset_path, "supplier.parquet"), filesystem=fs)

    # intervalo de datas
    shipdate_from = datetime.strptime("1996-01-01", "%Y-%m-%d")
    shipdate_to = datetime.strptime("1996-04-01", "%Y-%m-%d")

    # filtrar lineitem e calcular receita
    lineitem_filtered = lineitem[
        (lineitem.l_shipdate >= shipdate_from) & (lineitem.l_shipdate < shipdate_to)
    ]
    lineitem_filtered["revenue"] = lineitem_filtered.l_extendedprice * (1 - lineitem_filtered.l_discount)

    # agrupar por supplier
    revenue = (
        lineitem_filtered.groupby("l_suppkey")
        .revenue.sum()
        .to_frame()
        .reset_index()
        .rename(columns={"revenue": "total_revenue", "l_suppkey": "supplier_no"})
    )

    # merge com supplier e filtrar o maior total_revenue
    table = supplier.merge(
        revenue, left_on="s_suppkey", right_on="supplier_no", how="inner"
    )

    result = table[table.total_revenue == revenue.total_revenue.max()][
        ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
    ].sort_values(by="s_suppkey")

    return result

result = query_15(dataset_path, fs, SCALE)
#print(result.compute())

def query_16(dataset_path, fs, SCALE):
    """
    Query 16: Contagem de suppliers distintos por part, filtrando por marcas, tipos e tamanho, 
    excluindo suppliers com comentários de reclamações de clientes.
    """
    import numpy as np

    partsupp = dd.read_parquet(os.path.join(dataset_path, "partsupp.parquet"), filesystem=fs)
    part = dd.read_parquet(os.path.join(dataset_path, "part.parquet"), filesystem=fs)
    supplier = dd.read_parquet(os.path.join(dataset_path, "supplier.parquet"), filesystem=fs)

    # identificar suppliers com comentários de reclamações
    supplier["is_complaint"] = supplier.s_comment.str.contains("Customer.*Complaints", regex=True)
    complaint_suppkeys = supplier[supplier.is_complaint].s_suppkey.compute().tolist()

    # filtrar partsupp removendo suppliers com reclamações
    partsupp_filtered = partsupp[~partsupp.ps_suppkey.isin(complaint_suppkeys)]

    # merge com part
    table = partsupp_filtered.merge(part, left_on="ps_partkey", right_on="p_partkey", how="inner")

    # aplicar filtros de marca, tipo e tamanho
    table = table[
        (table.p_brand != "Brand#45")
        & (~table.p_type.str.startswith("MEDIUM POLISHED"))
        & (table.p_size.isin([49, 14, 23, 45, 19, 3, 36, 9]))
    ]

    # agrupar e contar suppliers distintos
    result = (
        table.groupby(["p_brand", "p_type", "p_size"])
        .ps_suppkey.nunique()
        .reset_index()
        .rename(columns={"ps_suppkey": "supplier_cnt"})
        .sort_values(
            by=["supplier_cnt", "p_brand", "p_type", "p_size"],
            ascending=[False, True, True, True],
        )
    )

    return result

result = query_16(dataset_path, fs, SCALE)
#print(result.compute())

def query_17(dataset_path, fs, SCALE):
    """
    Query 17: Calcula a média anual de l_extendedprice para parts da marca Brand#23,
    contêiner MED BOX e quantidade menor que 20% da média por partkey.
    """
    lineitem = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)
    part = dd.read_parquet(os.path.join(dataset_path, "part.parquet"), filesystem=fs)

    # merge lineitem com part
    joined = lineitem.merge(part, left_on="l_partkey", right_on="p_partkey", how="inner")

    # filtrar por marca e container
    joined = joined[(joined.p_brand == "Brand#23") & (joined.p_container == "MED BOX")]

    # calcular média de quantidade por partkey
    avg_qnty_by_partkey = (
        joined.groupby("l_partkey")["l_quantity"]
        .mean()
        .to_frame()
        .rename(columns={"l_quantity": "l_quantity_avg"})
    )

    # merge com a tabela principal
    table = joined.merge(avg_qnty_by_partkey, left_on="l_partkey", right_index=True, how="left")

    # filtrar linhas com quantidade menor que 20% da média
    table = table[table.l_quantity < 0.2 * table.l_quantity_avg]

    # calcular média anual
    return ((table.l_extendedprice.to_frame().sum() / 7.0).round(2)).to_frame("avg_yearly")

result = query_17(dataset_path, fs, SCALE)
#print(result.compute())

'''
def query_18(dataset_path, fs, SCALE):

    customer = dd.read_parquet(os.path.join(dataset_path, "customer.parquet"), filesystem=fs, columns=None)
    orders = dd.read_parquet(os.path.join(dataset_path, "orders.parquet"), filesystem=fs, columns=None)
    lineitem = dd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"), filesystem=fs)

    qnt_over_300 = (lineitem.groupby("l_orderkey")["l_quantity"].sum().reset_index())
    qnt_over_300 = qnt_over_300[qnt_over_300.l_quantity > 300]

    # merge orders filtrados com lineitem e customer
    table = (
        orders.merge(qnt_over_300, left_on="o_orderkey", right_on="l_orderkey", how="inner")
        .merge(lineitem, left_on="o_orderkey", right_on="l_orderkey", how="inner")
        .merge(customer, left_on="o_custkey", right_on="c_custkey", how="inner")
    )

    # agrupar e somar quantidade
    result = (
        table.groupby(
            ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"]
        )["l_quantity"]
        .sum()
        .reset_index()
        .rename(columns={"l_quantity": "sum_quantity"})
        .sort_values(by=["o_totalprice", "o_orderdate"], ascending=[False, True])
        .head(100, compute=False)
    )

    return result
'''

def query_19(dataset_path, fs, SCALE):
    import dask.dataframe as dd

    lineitem = dd.read_parquet(dataset_path + "lineitem.parquet", filesystem=fs)
    part = dd.read_parquet(dataset_path + "part.parquet", filesystem=fs)

    # Merge lineitem e part
    table = lineitem.merge(part, left_on="l_partkey", right_on="p_partkey", how="inner")

    # Condições para os três grupos
    cond1 = (
        (table.p_brand == "Brand#12") &
        (table.p_container.isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])) &
        (table.l_quantity.between(1, 11)) &
        (table.p_size.between(1, 5)) &
        (table.l_shipmode.isin(["AIR", "AIR REG"])) &
        (table.l_shipinstruct == "DELIVER IN PERSON")
    )

    cond2 = (
        (table.p_brand == "Brand#23") &
        (table.p_container.isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])) &
        (table.l_quantity.between(10, 20)) &
        (table.p_size.between(1, 10)) &
        (table.l_shipmode.isin(["AIR", "AIR REG"])) &
        (table.l_shipinstruct == "DELIVER IN PERSON")
    )

    cond3 = (
        (table.p_brand == "Brand#34") &
        (table.p_container.isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])) &
        (table.l_quantity.between(20, 30)) &
        (table.p_size.between(1, 15)) &
        (table.l_shipmode.isin(["AIR", "AIR REG"])) &
        (table.l_shipinstruct == "DELIVER IN PERSON")
    )

    # Filtra linhas que atendem qualquer uma das três condições
    filtered_table = table[cond1 | cond2 | cond3]

    # Calcula receita
    filtered_table["revenue"] = filtered_table.l_extendedprice * (1 - filtered_table.l_discount)

    # Retorna resultado agregado
    return filtered_table[["revenue"]].sum().round(2).to_frame("revenue")

result = query_19(dataset_path, fs, SCALE)
#print(result.compute())

def query_20(dataset_path, fs, SCALE):
    lineitem = dd.read_parquet(dataset_path + "lineitem.parquet", filesystem=fs)
    supplier = dd.read_parquet(dataset_path + "supplier.parquet", filesystem=fs)
    nation = dd.read_parquet(dataset_path + "nation.parquet", filesystem=fs)
    part = dd.read_parquet(dataset_path + "part.parquet", filesystem=fs)
    partsupp = dd.read_parquet(dataset_path + "partsupp.parquet", filesystem=fs)

    # Período de 1994
    shipdate_from = datetime.strptime("1994-01-01", "%Y-%m-%d")
    shipdate_to = datetime.strptime("1995-01-01", "%Y-%m-%d")

    # Soma da quantidade por partkey e suppkey
    line_sum = (
        lineitem[(lineitem.l_shipdate >= shipdate_from) & (lineitem.l_shipdate < shipdate_to)]
        .groupby(["l_suppkey", "l_partkey"])
        .l_quantity.sum()
        .rename("sum_quantity")
        .reset_index()
    )
    line_sum["sum_quantity"] = line_sum["sum_quantity"] * 0.5

    # Filtra fornecedores no Canadá
    canada_nation = nation[nation.n_name == "CANADA"]
    supplier_canada = supplier.merge(canada_nation, left_on="s_nationkey", right_on="n_nationkey")

    # Filtra partes cujo nome começa com 'forest'
    forest_parts = part[part.p_name.str.strip().str.startswith("forest")]

    # Merge partsupp com forest_parts e linha de soma de quantidade
    q_final = (
        partsupp.merge(forest_parts, left_on="ps_partkey", right_on="p_partkey", how="inner")
        .merge(line_sum, left_on=["ps_suppkey", "ps_partkey"], right_on=["l_suppkey", "l_partkey"], how="inner")
    )

    # Filtra partsupp com quantidade disponível maior que 0.5 * soma
    q_final = q_final[q_final.ps_availqty > q_final.sum_quantity]

    # Merge final com fornecedores do Canadá
    q_final = supplier_canada.merge(q_final, left_on="s_suppkey", right_on="ps_suppkey", how="inner")
    q_final["s_address"] = q_final["s_address"].str.strip()

    return q_final[["s_name", "s_address"]].sort_values("s_name")

result = query_20(dataset_path, fs, SCALE)
#print(result.compute())

'''
def query_21(dataset_path, fs, SCALE):
    supplier = dd.read_parquet(dataset_path + "supplier.parquet", filesystem=fs)
    lineitem = dd.read_parquet(dataset_path + "lineitem.parquet", filesystem=fs)
    orders = dd.read_parquet(dataset_path + "orders.parquet", filesystem=fs)
    nation = dd.read_parquet(dataset_path + "nation.parquet", filesystem=fs)

    NATION = "SAUDI ARABIA"

    # L1: pedidos com mais de um fornecedor e atraso
    l1 = lineitem[lineitem.l_receiptdate > lineitem.l_commitdate]
    l1_counts = l1.groupby("l_orderkey")["l_suppkey"].nunique().reset_index()
    l1_counts = l1_counts[l1_counts.l_suppkey > 1]

    l1_filtered = l1.merge(l1_counts, on="l_orderkey", how="inner")

    # Merge com orders, supplier e nation
    table = (
        l1_filtered.merge(orders, left_on="l_orderkey", right_on="o_orderkey")
        .merge(supplier, left_on="l_suppkey", right_on="s_suppkey")
        .merge(nation, left_on="s_nationkey", right_on="n_nationkey")
    )

    # Aplica filtros finais
    table = table[(table.o_orderstatus == "F") & (table.n_name == NATION)]

    return (
        table.groupby("s_name")
        .size()
        .rename("numwait")
        .reset_index()
        .sort_values(["numwait", "s_name"], ascending=[False, True])
        .head(100, compute=False)
    )

result = query_21(dataset_path, fs, SCALE)
#print(result.compute())


def query_22(dataset_path, fs, SCALE):
    customer = dd.read_parquet(dataset_path + "customer.parquet", filesystem=fs)
    orders = dd.read_parquet(dataset_path + "orders.parquet", filesystem=fs)

    # Filtra clientes por código de país
    customer["cntrycode"] = customer.c_phone.str.strip().str.slice(0, 2)
    valid_codes = ["13", "31", "23", "29", "30", "18", "17"]
    customer = customer[customer.cntrycode.isin(valid_codes)]

    # Média do saldo positivo
    avg_acctbal = customer[customer.c_acctbal > 0]["c_acctbal"].mean()

    # Seleciona clientes com saldo acima da média e sem pedidos
    custsale = customer[customer.c_acctbal > avg_acctbal]
    custsale = custsale.merge(orders, left_on="c_custkey", right_on="o_custkey", how="left")
    custsale = custsale[custsale.o_custkey.isnull()]

    # Agrupamento final
    result = custsale.groupby("cntrycode").agg({"c_acctbal": ["size", "sum"]})
    result.columns = ["numcust", "totacctbal"]

    return result.reset_index().sort_values("cntrycode")
    
result = query_22(dataset_path, fs, SCALE)
#print(result.compute())
'''

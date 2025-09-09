from datetime import date
import modin.pandas as pd
import os
import warnings
import modin.config as cfg

#from dask.distributed import Client
#client = Client(memory_limit=None)  # Sem limite

#Desativa warnings do modin
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

N_CORES = int(os.environ.get("N_CORES", "1"))
SCALE = str(os.environ.get("SCALE_FACTOR", "1"))
MODIN_ENGINE = str(os.environ.get("MODIN_ENGINE", "python"))
STORAGE_FORMAT = str(os.environ.get("STORAGE_FORMAT", ""))
MODIN_MEMORY = os.environ.get("MODIN_MEMORY", "")

cfg.Engine.put(MODIN_ENGINE)
cfg.StorageFormat.put(STORAGE_FORMAT)
#cfg.NPartitions.put(int(N_CORES))
#cfg.Memory.put(MODIN_MEMORY)


dataset_path = f"data_tbl/scale-{SCALE}/"


def query_01(dataset_path, SCALE):
    VAR1 = date(1998, 9, 2)

    lineitem_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    lineitem_filtered = lineitem_ds[lineitem_ds['l_shipdate'] <= VAR1]

    total = lineitem_filtered.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('l_quantity', 'sum'),
        sum_base_price=('l_extendedprice', 'sum'),
        sum_disc_price=('l_extendedprice', lambda x: (x * (1 - lineitem_filtered.loc[x.index, 'l_discount'])).sum()),
        sum_charge=('l_extendedprice', lambda x: (x * (1 - lineitem_filtered.loc[x.index, 'l_discount']) * (1 + lineitem_filtered.loc[x.index, 'l_tax'])).sum()),
        avg_qty=('l_quantity', 'mean'),
        avg_price=('l_extendedprice', 'mean'),
        avg_disc=('l_discount', 'mean'),
        count_order=('l_orderkey', 'count')
    )

    return total.reset_index().sort_values(['l_returnflag', 'l_linestatus'])

#resulto = query_01(dataset_path, SCALE)
#print(f"Pandas Query 01 : \n{result}")

def query_02(dataset_path):
    var_1 = 15
    var_2 = "BRASS"
    var_3 = "EUROPE"

    region_ds = pd.read_parquet(os.path.join(dataset_path, "region.parquet"))
    nation_ds = pd.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    part_ds = pd.read_parquet(os.path.join(dataset_path, "part.parquet"))
    part_supp_ds = pd.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))

    # Filtro principal
    main_query = part_ds.merge(part_supp_ds, left_on="p_partkey", right_on="ps_partkey") \
                       .merge(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey") \
                       .merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey") \
                       .merge(region_ds, left_on="n_regionkey", right_on="r_regionkey") \
                       .query(f"p_size == {var_1} and p_type.str.endswith('{var_2}') and r_name == '{var_3}'")

    # Subquery para encontrar o custo mínimo por partkey
    subquery = part_supp_ds.merge(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey") \
                          .merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey") \
                          .merge(region_ds, left_on="n_regionkey", right_on="r_regionkey") \
                          .query(f"r_name == '{var_3}'") \
                          .groupby('ps_partkey')['ps_supplycost'].min().reset_index()

    # Junção com a subquery
    result = main_query.merge(subquery, left_on=['p_partkey', 'ps_supplycost'], right_on=['ps_partkey', 'ps_supplycost'])
    
    final_cols = ['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 
                 's_address', 's_phone', 's_comment']
    
    result = result[final_cols] \
        .sort_values(['s_acctbal', 'n_name', 's_name', 'p_partkey'], 
                    ascending=[False, True, True, True]) \
        .head(100)
    
    for col in result.select_dtypes(include='object'):
        result[col] = result[col].str.strip()
    
    return result

#resulto = query_02(dataset_path)
#print(f"Pandas Query 02 : \n{result}")


def query_03(dataset_path):
    var_1 = var_2 = date(1995, 3, 15)
    var_3 = "BUILDING"

    customer_ds = pd.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    result = customer_ds[customer_ds['c_mktsegment'] == var_3] \
        .merge(orders_ds, left_on='c_custkey', right_on='o_custkey') \
        .merge(line_item_ds, left_on='o_orderkey', right_on='l_orderkey') \
        .query('o_orderdate < @var_2 and l_shipdate > @var_1')
    
    result['revenue'] = result['l_extendedprice'] * (1 - result['l_discount'])
    
    result = result.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']) \
        .agg(revenue=('revenue', 'sum')) \
        .reset_index() \
        .sort_values(['revenue', 'o_orderdate'], ascending=[False, True]) \
        .head(10)
    
    return result[['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority']]

#resulto = query_03(dataset_path)
#print(f"Pandas Query 03 : \n{result}")

def query_04(dataset_path):
    var_1 = date(1993, 7, 1)
    var_2 = date(1993, 10, 1)

    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    result = line_item_ds.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
    result = result[(result['o_orderdate'] >= var_1) & (result['o_orderdate'] < var_2) & 
                   (result['l_commitdate'] < result['l_receiptdate'])]
    
    result = result.drop_duplicates(subset=['o_orderpriority', 'l_orderkey'])
    
    result = result.groupby('o_orderpriority') \
                  .size() \
                  .reset_index(name='order_count') \
                  .sort_values('o_orderpriority')
    
    result['order_count'] = result['order_count'].astype('int64')
    
    return result

#resulto = query_04(dataset_path)
#print(f"Pandas Query 04 : \n{result}")

def query_05(dataset_path):
    var_1 = "ASIA"
    var_2 = date(1994, 1, 1)
    var_3 = date(1995, 1, 1)

    region_ds = pd.read_parquet(os.path.join(dataset_path, "region.parquet"))
    nation_ds = pd.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    customer_ds = pd.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))

    result = region_ds.merge(nation_ds, left_on="r_regionkey", right_on="n_regionkey") \
                     .merge(customer_ds, left_on="n_nationkey", right_on="c_nationkey") \
                     .merge(orders_ds, left_on="c_custkey", right_on="o_custkey") \
                     .merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey") \
                     .merge(supplier_ds, left_on=["l_suppkey", "n_nationkey"], right_on=["s_suppkey", "s_nationkey"]) \
                     .query('r_name == @var_1 and o_orderdate >= @var_2 and o_orderdate < @var_3')
    
    result['revenue'] = result['l_extendedprice'] * (1 - result['l_discount'])
    
    result = result.groupby('n_name')['revenue'].sum().reset_index() \
                  .sort_values('revenue', ascending=False)
    
    return result
#resulto = query_05(dataset_path)
#print(f"Pandas Query 05 : \n{result}")

def query_06(dataset_path):
    var_1 = date(1994, 1, 1)
    var_2 = date(1995, 1, 1)
    var_3 = 24

    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    
    result = line_item_ds[
        (line_item_ds['l_shipdate'] >= var_1) & 
        (line_item_ds['l_shipdate'] < var_2) &
        (line_item_ds['l_discount'] >= 0.05) & 
        (line_item_ds['l_discount'] <= 0.07) &
        (line_item_ds['l_quantity'] < var_3)
    ]
    
    #result['revenue'] = result['l_extendedprice'] * result['l_discount']
    result = result.assign(revenue=result['l_extendedprice'] * result['l_discount'])
    total_revenue = result['revenue'].sum()
    
    return pd.DataFrame({'revenue': [total_revenue]})

#resulto = query_06(dataset_path)
#print(f"Pandas Query 06 : \n{result}")

def query_07(dataset_path):
    nation_ds = pd.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    customer_ds = pd.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))

    n1 = nation_ds[nation_ds['n_name'] == "FRANCE"]
    n2 = nation_ds[nation_ds['n_name'] == "GERMANY"]

    var_1 = date(1995, 1, 1)
    var_2 = date(1996, 12, 31)

    # Primeira combinação
    df1 = customer_ds.merge(n1, left_on="c_nationkey", right_on="n_nationkey") \
                    .merge(orders_ds, left_on="c_custkey", right_on="o_custkey") \
                    .rename(columns={"n_name": "cust_nation"}) \
                    .merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey") \
                    .merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey") \
                    .merge(n2, left_on="s_nationkey", right_on="n_nationkey") \
                    .rename(columns={"n_name": "supp_nation"})

    # Segunda combinação
    df2 = customer_ds.merge(n2, left_on="c_nationkey", right_on="n_nationkey") \
                    .merge(orders_ds, left_on="c_custkey", right_on="o_custkey") \
                    .rename(columns={"n_name": "cust_nation"}) \
                    .merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey") \
                    .merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey") \
                    .merge(n1, left_on="s_nationkey", right_on="n_nationkey") \
                    .rename(columns={"n_name": "supp_nation"})

    result = pd.concat([df1, df2])
    result = result[(result['l_shipdate'] >= var_1) & (result['l_shipdate'] <= var_2)]
    
    result = result.assign(
        volume=result['l_extendedprice'] * (1 - result['l_discount']),
        l_year=pd.to_datetime(result['l_shipdate']).dt.year
    )
    
    result = result.groupby(['supp_nation', 'cust_nation', 'l_year']) \
                  .agg(revenue=('volume', 'sum')) \
                  .reset_index() \
                  .sort_values(['supp_nation', 'cust_nation', 'l_year'])
    
    return result

#resulto = query_07(dataset_path)
#print(f"Pandas Query 07 : \n{result}")

def query_08(dataset_path):
    part_ds = pd.read_parquet(os.path.join(dataset_path, "part.parquet"))
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    customer_ds = pd.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    nation_ds = pd.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    region_ds = pd.read_parquet(os.path.join(dataset_path, "region.parquet"))

    n1 = nation_ds[['n_nationkey', 'n_regionkey']]
    n2 = nation_ds[['n_nationkey', 'n_name']]

    result = part_ds.merge(line_item_ds, left_on="p_partkey", right_on="l_partkey") \
                   .merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey") \
                   .merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey") \
                   .merge(customer_ds, left_on="o_custkey", right_on="c_custkey") \
                   .merge(n1, left_on="c_nationkey", right_on="n_nationkey") \
                   .merge(region_ds, left_on="n_regionkey", right_on="r_regionkey") \
                   .query('r_name == "AMERICA"') \
                   .merge(n2, left_on="s_nationkey", right_on="n_nationkey")
    
    result = result[(result['o_orderdate'] >= date(1995, 1, 1)) & 
                   (result['o_orderdate'] <= date(1996, 12, 31)) &
                   (result['p_type'] == "ECONOMY ANODIZED STEEL")]

    result = result.assign(
        o_year=result['o_orderdate'].apply(lambda x: x.year),
        volume=result['l_extendedprice'] * (1 - result['l_discount'])
    )
    
    result['_tmp'] = result.apply(lambda x: x['volume'] if x['n_name'] == "BRAZIL" else 0, axis=1)

    # Correção do groupby
    sum_tmp = result.groupby('o_year')['_tmp'].sum()
    sum_volume = result.groupby('o_year')['volume'].sum()
    result = (sum_tmp / sum_volume).round(2).reset_index(name='mkt_share').sort_values('o_year')

    return result

#resulto = query_08(dataset_path)
#print(f"Pandas Query 08 : \n{result}")

def query_09(dataset_path):
    part_ds = pd.read_parquet(os.path.join(dataset_path, "part.parquet"))
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    part_supp_ds = pd.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    nation_ds = pd.read_parquet(os.path.join(dataset_path, "nation.parquet"))

    result = line_item_ds.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey") \
                        .merge(part_supp_ds, left_on=["l_suppkey", "l_partkey"], right_on=["ps_suppkey", "ps_partkey"]) \
                        .merge(part_ds, left_on="l_partkey", right_on="p_partkey") \
                        .merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey") \
                        .merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey") \
                        .query('p_name.str.contains("green")', engine='python')

    result = result.assign(
        o_year=result['o_orderdate'].apply(lambda x: x.year),
        amount=result['l_extendedprice'] * (1 - result['l_discount']) - result['ps_supplycost'] * result['l_quantity'],
        nation=result['n_name']  # Adicionando a coluna nation explicitamente
    )

    result = result.groupby(['nation', 'o_year']) \
                  .agg(sum_profit=('amount', 'sum')) \
                  .round(2) \
                  .reset_index() \
                  .sort_values(['nation', 'o_year'], ascending=[True, False])

    return result

#resulto = query_09(dataset_path)
#print(f"Pandas Query 09 : \n{result}")

def query_10(dataset_path):
    customer_ds = pd.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    nation_ds = pd.read_parquet(os.path.join(dataset_path, "nation.parquet"))

    var_1 = date(1993, 10, 1)
    var_2 = date(1994, 1, 1)

    result = customer_ds.merge(orders_ds, left_on="c_custkey", right_on="o_custkey") \
                       .merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey") \
                       .merge(nation_ds, left_on="c_nationkey", right_on="n_nationkey") \
                       .query('o_orderdate >= @var_1 and o_orderdate < @var_2 and l_returnflag == "R"')

    result = result.assign(
        revenue=result['l_extendedprice'] * (1 - result['l_discount'])
    )

    result = result.groupby([
        'c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment'
    ])['revenue'].sum().round(2).reset_index()

    result['c_address'] = result['c_address'].str.strip()
    result['c_comment'] = result['c_comment'].str.strip()

    result = result[[
        'c_custkey', 'c_name', 'revenue', 'c_acctbal', 'n_name', 
        'c_address', 'c_phone', 'c_comment'
    ]].sort_values('revenue', ascending=False).head(20)

    return result

#resulto = query_10(dataset_path)
#print(f"Pandas Query 10 : \n{result}")

def query_11(dataset_path, scale):
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    part_supp_ds = pd.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))
    nation_ds = pd.read_parquet(os.path.join(dataset_path, "nation.parquet"))

    var_1 = "GERMANY"
    var_2 = 0.0001 / scale

    # Primeira parte: join das tabelas e filtro por nação
    res_1 = part_supp_ds.merge(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey") \
                       .merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey") \
                       .query('n_name == @var_1')

    # Segunda parte: cálculo do valor total
    total_value = (res_1['ps_supplycost'] * res_1['ps_availqty']).sum().round(2) * var_2

    # Terceira parte: agrupamento por partkey e filtro
    result = res_1.groupby('ps_partkey') \
                 .agg(value=('ps_supplycost', lambda x: (x * res_1.loc[x.index, 'ps_availqty']).sum().round(2))) \
                 .reset_index() \
                 .query('value > @total_value') \
                 .sort_values('value', ascending=False)

    return result
#resulto = query_11(dataset_path, int(SCALE))
#print(f"Pandas Query 11 : \n{result}")

def query_12(dataset_path):
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    var_1 = "MAIL"
    var_2 = "SHIP"
    var_3 = date(1994, 1, 1)
    var_4 = date(1995, 1, 1)

    result = orders_ds.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey") \
                     .query('l_shipmode in [@var_1, @var_2]') \
                     .query('l_commitdate < l_receiptdate') \
                     .query('l_shipdate < l_commitdate') \
                     .query('l_receiptdate >= @var_3 and l_receiptdate < @var_4')

    result = result.assign(
        high_line_count=result['o_orderpriority'].isin(["1-URGENT", "2-HIGH"]).astype(int),
        low_line_count=(~result['o_orderpriority'].isin(["1-URGENT", "2-HIGH"])).astype(int)
    )

    result = result.groupby('l_shipmode') \
                  .agg(high_line_count=('high_line_count', 'sum'),
                       low_line_count=('low_line_count', 'sum')) \
                  .reset_index() \
                  .sort_values('l_shipmode')

    return result

#resulto = query_12(dataset_path)
#print(f"Pandas Query 12 : \n{result}")

def query_13(dataset_path):
    var_1 = "special"
    var_2 = "requests"

    customer_ds = pd.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    
    # Aplicar a condição no merge (left join com condição adicional)
    result = customer_ds.merge(
        orders_ds[~orders_ds['o_comment'].str.contains(f"{var_1}.*{var_2}", na=False)], 
        left_on="c_custkey", 
        right_on="o_custkey", 
        how="left"
    )
    
    # Primeiro groupby: contar orders por customer
    c_orders = result.groupby("c_custkey") \
                    .agg(c_count=('o_orderkey', 'count')) \
                    .reset_index()
    
    # Segundo groupby: contar distribuição de customers por c_count
    result_final = c_orders.groupby("c_count") \
                          .agg(custdist=('c_custkey', 'count')) \
                          .reset_index() \
                          .sort_values(['custdist', 'c_count'], ascending=[False, False])
    
    return result_final

#resulto = query_13(dataset_path)
#print(f"Pandas Query 13 : \n{result}")

def query_14(dataset_path):
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    part_ds = pd.read_parquet(os.path.join(dataset_path, "part.parquet"))

    var_1 = date(1995, 9, 1)
    var_2 = date(1995, 10, 1)

    result = line_item_ds.merge(part_ds, left_on="l_partkey", right_on="p_partkey") \
                        .query('l_shipdate >= @var_1 and l_shipdate < @var_2')

    # Primeiro calcular discounted_price
    result = result.assign(
        discounted_price=result['l_extendedprice'] * (1 - result['l_discount'])
    )
    
    # Depois calcular promo_revenue
    result = result.assign(
        promo_revenue=result.apply(lambda x: x['discounted_price'] if x['p_type'].startswith('PROMO') else 0, axis=1)
    )

    total_promo = result['promo_revenue'].sum()
    total_discounted = result['discounted_price'].sum()
    
    promo_revenue = (100.00 * total_promo / total_discounted).round(2)
    
    return pd.DataFrame({'promo_revenue': [promo_revenue]})

#resulto = query_14(dataset_path)
#print(f"Pandas Query 14 : \n{result}")

def query_15(dataset_path):
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))

    var_1 = date(1996, 1, 1)
    var_2 = date(1996, 4, 1)

    # Criar a view revenue
    revenue_ds = line_item_ds.query('l_shipdate >= @var_1 and l_shipdate < @var_2') \
                           .assign(total_revenue=lambda x: x['l_extendedprice'] * (1 - x['l_discount'])) \
                           .groupby('l_suppkey') \
                           .agg(total_revenue=('total_revenue', 'sum')) \
                           .reset_index() \
                           .rename(columns={'l_suppkey': 'supplier_no'})

    # Encontrar o máximo total_revenue
    max_revenue = revenue_ds['total_revenue'].max()

    # Juntar com supplier e filtrar
    result = supplier_ds.merge(revenue_ds, left_on='s_suppkey', right_on='supplier_no') \
                       .query('total_revenue == @max_revenue') \
                       .assign(total_revenue=lambda x: x['total_revenue'].round(2)) \
                       [['s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue']] \
                       .sort_values('s_suppkey')

    return result

#resulto = query_15(dataset_path)
#print(f"Pandas Query 15 : \n{result}")

def query_16(dataset_path):
    part_supp_ds = pd.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))
    part_ds = pd.read_parquet(os.path.join(dataset_path, "part.parquet"))
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))

    var_1 = "Brand#45"
    sizes = [49, 14, 23, 45, 19, 3, 36, 9]

    # Filtrar suppliers com complaints
    supplier_complaints = supplier_ds[supplier_ds['s_comment'].str.contains("Customer.*Complaints", na=False)]['s_suppkey']

    # Join part e partsupp
    result = part_ds.merge(part_supp_ds, left_on="p_partkey", right_on="ps_partkey") \
                   .query('p_brand != @var_1') \
                   .query('not p_type.str.startswith("MEDIUM POLISHED")') \
                   .query('p_size in @sizes') \
                   .query('ps_suppkey not in @supplier_complaints')

    # Agrupar e contar suppliers distintos
    result = result.groupby(['p_brand', 'p_type', 'p_size']) \
                  .agg(supplier_cnt=('ps_suppkey', 'nunique')) \
                  .reset_index() \
                  .sort_values(['supplier_cnt', 'p_brand', 'p_type', 'p_size'], 
                              ascending=[False, True, True, True])

    return result

#resulto = query_16(dataset_path)
#print(f"Pandas Query 16 : \n{result}")

def query_17(dataset_path):
    var_1 = "Brand#23"
    var_2 = "MED BOX"

    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    part_ds = pd.read_parquet(os.path.join(dataset_path, "part.parquet"))

    # Filtrar partes pela marca e container
    filtered_parts = part_ds.query('p_brand == @var_1 and p_container == @var_2')

    # Calcular a média de quantidade por partkey (0.2 * avg)
    avg_quantity = line_item_ds.groupby('l_partkey')['l_quantity'].mean() * 0.2
    avg_quantity_df = avg_quantity.reset_index(name='avg_quantity')

    # Juntar com partes filtradas
    result = filtered_parts.merge(line_item_ds, left_on='p_partkey', right_on='l_partkey') \
                          .merge(avg_quantity_df, left_on='p_partkey', right_on='l_partkey') \
                          .query('l_quantity < avg_quantity')

    # Calcular o resultado final
    avg_yearly = (result['l_extendedprice'].sum() / 7.0).round(2)
    
    return pd.DataFrame({'avg_yearly': [avg_yearly]})

#resulto = query_17(dataset_path)
#print(f"Pandas Query 17 : \n{result}")

def query_18(dataset_path):
    customer_ds = pd.read_parquet(os.path.join(dataset_path, "customer.parquet"))
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    var_1 = 300

    # Encontrar orders com quantidade total > 300
    order_totals = line_item_ds.groupby('l_orderkey')['l_quantity'].sum().reset_index()
    large_orders = order_totals[order_totals['l_quantity'] > var_1]['l_orderkey']

    # Filtrar e juntar dados
    result = orders_ds[orders_ds['o_orderkey'].isin(large_orders)] \
        .merge(customer_ds, left_on='o_custkey', right_on='c_custkey') \
        .merge(line_item_ds, left_on='o_orderkey', right_on='l_orderkey') \
        .groupby(['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice']) \
        .agg(col6=('l_quantity', 'sum')) \
        .reset_index() \
        [['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice', 'col6']] \
        .sort_values(['o_totalprice', 'o_orderdate'], ascending=[False, False]) \
        .head(100)

    return result

#resulto = query_18(dataset_path)
#print(f"Pandas Query 18 : \n{result}")

def query_19(dataset_path):
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    part_ds = pd.read_parquet(os.path.join(dataset_path, "part.parquet"))

    result = part_ds.merge(line_item_ds, left_on="p_partkey", right_on="l_partkey") \
                   .query('l_shipmode in ["AIR", "AIR REG"] and l_shipinstruct == "DELIVER IN PERSON"')

    # Aplicar as condições complexas com OR
    condition1 = (result['p_brand'] == "Brand#12") & \
                (result['p_container'].isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])) & \
                (result['l_quantity'] >= 1) & (result['l_quantity'] <= 11) & \
                (result['p_size'] >= 1) & (result['p_size'] <= 5)

    condition2 = (result['p_brand'] == "Brand#23") & \
                (result['p_container'].isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])) & \
                (result['l_quantity'] >= 10) & (result['l_quantity'] <= 20) & \
                (result['p_size'] >= 1) & (result['p_size'] <= 10)

    condition3 = (result['p_brand'] == "Brand#34") & \
                (result['p_container'].isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])) & \
                (result['l_quantity'] >= 20) & (result['l_quantity'] <= 30) & \
                (result['p_size'] >= 1) & (result['p_size'] <= 15)

    result = result[condition1 | condition2 | condition3]

    revenue = (result['l_extendedprice'] * (1 - result['l_discount'])).sum().round(2)
    
    return pd.DataFrame({'revenue': [revenue]})

#resulto = query_19(dataset_path)
#print(f"Pandas Query 19 : \n{result}") 

def query_20(dataset_path):
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    nation_ds = pd.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    part_ds = pd.read_parquet(os.path.join(dataset_path, "part.parquet"))
    part_supp_ds = pd.read_parquet(os.path.join(dataset_path, "partsupp.parquet"))

    var_1 = date(1994, 1, 1)
    var_2 = date(1995, 1, 1)
    var_3 = "CANADA"
    var_4 = "forest"

    # Calcular sum_quantity (corrigido)
    res_1 = line_item_ds.query('l_shipdate >= @var_1 and l_shipdate < @var_2') \
                      .groupby(['l_partkey', 'l_suppkey']) \
                      .agg(sum_quantity=('l_quantity', 'sum')) \
                      .reset_index()
    res_1['sum_quantity'] = res_1['sum_quantity'] * 0.5

    # Filtrar nation
    res_2 = nation_ds[nation_ds['n_name'] == var_3]
    res_3 = supplier_ds.merge(res_2, left_on="s_nationkey", right_on="n_nationkey")

    # Filtrar parts e aplicar condições
    filtered_parts = part_ds[part_ds['p_name'].str.startswith(var_4)]['p_partkey'].unique()
    
    result = part_supp_ds[part_supp_ds['ps_partkey'].isin(filtered_parts)] \
                       .merge(res_1, left_on=['ps_suppkey', 'ps_partkey'], right_on=['l_suppkey', 'l_partkey']) \
                       .query('ps_availqty > sum_quantity') \
                       .merge(res_3, left_on='ps_suppkey', right_on='s_suppkey') \
                       .assign(s_address=lambda x: x['s_address'].str.strip()) \
                       [['s_name', 's_address']] \
                       .sort_values('s_name')

    return result

#resulto = query_20(dataset_path)
#print(f"Pandas Query 20 : \n{result}")

def query_21(dataset_path):
    line_item_ds = pd.read_parquet(os.path.join(dataset_path, "lineitem.parquet"))
    supplier_ds = pd.read_parquet(os.path.join(dataset_path, "supplier.parquet"))
    nation_ds = pd.read_parquet(os.path.join(dataset_path, "nation.parquet"))
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))

    var_1 = "SAUDI ARABIA"

    # Orders com mais de 1 supplier
    multi_supplier_orders = line_item_ds.groupby('l_orderkey')['l_suppkey'].nunique()
    multi_supplier_orders = multi_supplier_orders[multi_supplier_orders > 1].index

    # Lineitems com recebimento após commit
    late_items = line_item_ds[line_item_ds['l_receiptdate'] > line_item_ds['l_commitdate']]
    late_items = late_items[late_items['l_orderkey'].isin(multi_supplier_orders)]

    # Para cada order, verificar se há apenas 1 supplier com atraso
    result = late_items.groupby('l_orderkey')['l_suppkey'].nunique().reset_index()
    single_late_supplier = result[result['l_suppkey'] == 1]['l_orderkey']

    # Juntar todos os dados
    final_result = late_items[late_items['l_orderkey'].isin(single_late_supplier)] \
        .merge(supplier_ds, left_on='l_suppkey', right_on='s_suppkey') \
        .merge(nation_ds, left_on='s_nationkey', right_on='n_nationkey') \
        .merge(orders_ds, left_on='l_orderkey', right_on='o_orderkey') \
        .query('n_name == @var_1 and o_orderstatus == "F"') \
        .groupby('s_name') \
        .size().reset_index(name='numwait') \
        .sort_values(['numwait', 's_name'], ascending=[False, True]) \
        .head(100)

    return final_result

#resulto = query_21(dataset_path)
#print(f"Pandas Query 21 : \n{result}")

def query_22(dataset_path):
    orders_ds = pd.read_parquet(os.path.join(dataset_path, "orders.parquet"))
    customer_ds = pd.read_parquet(os.path.join(dataset_path, "customer.parquet"))

    # Extrair código do país
    res_1 = customer_ds.assign(
        cntrycode=customer_ds['c_phone'].str[:2]
    ).query('cntrycode in ["13", "31", "23", "29", "30", "18", "17"]')

    # Calcular média de saldo
    avg_bal = res_1[res_1['c_acctbal'] > 0.0]['c_acctbal'].mean()

    # Customers sem orders
    customers_with_orders = orders_ds['o_custkey'].unique()
    customers_no_orders = res_1[~res_1['c_custkey'].isin(customers_with_orders)]

    # Filtrar por saldo acima da média
    result = customers_no_orders[customers_no_orders['c_acctbal'] > avg_bal] \
        .groupby('cntrycode') \
        .agg(
            numcust=('c_custkey', 'count'),
            totacctbal=('c_acctbal', 'sum')
        ).round(2).reset_index() \
        .sort_values('cntrycode')

    return result

#resulto = query_22(dataset_path)
#print(f"Pandas Query 22 : \n{result}")

# Exemplo de uso para Modin
if __name__ == "__main__":
    result1 = query_01(dataset_path, SCALE)
    #print(f"Modin Query 01: {result1}")
    
    result2 = query_02(dataset_path) 
    #print(f"Modin Query 02: {result2}")

    result3 = query_03(dataset_path)
    #print(f"Modin Query 03: {result3}")
    
    result4 = query_04(dataset_path)
    #print(f"Modin Query 04: {result4}")

    result5 = query_05(dataset_path)
    #print(f"Modin Query 05: {result5}")

    result6 = query_06(dataset_path)
    #print(f"Modin Query 06: {result6}")

    result7 = query_07(dataset_path)
    #print(f"Modin Query 07: {result7}")

    result8 = query_08(dataset_path)
    #print(f"Modin Query 08: {result8}")

    result9 = query_09(dataset_path)
    #print(f"Modin Query 09: {result9}")

    result10 = query_10(dataset_path)
    #print(f"Modin Query 10: {result10}")

    result11 = query_11(dataset_path, SCALE)
    #print(f"Modin Query 11: {result11}")

    result12 = query_12(dataset_path)
    #print(f"Modin Query 12: {result12}")

    result13 = query_13(dataset_path)
    #print(f"Modin Query 13: {result13}")

    result14 = query_14(dataset_path)
    #print(f"Modin Query 14: {result14}")

    result15 = query_15(dataset_path)
    #print(f"Modin Query 15: {result15}")

    result16 = query_16(dataset_path)
    #print(f"Modin Query 16: {result16}")

    result17 = query_17(dataset_path)
    #print(f"Modin Query 17: {result17}")

    result18 = query_18(dataset_path)
    #print(f"Modin Query 18: {result18}")

    result19 = query_19(dataset_path)
    #print(f"Modin Query 19: {result19}")

    result20 = query_20(dataset_path)
    #print(f"Modin Query 20: {result20}")

    result21 = query_21(dataset_path)
    #print(f"Modin Query 21: {result21}")

    result22 = query_22(dataset_path)
    #print(f"Modin Query 22: {result22}")
from datetime import date
import pandas as pd
import os

SCALE = os.environ.get("SCALE_FACTOR", "1")
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

result = query_01(dataset_path, SCALE)
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

result = query_02(dataset_path)
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

result = query_03(dataset_path)
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

result = query_04(dataset_path)
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
result = query_05(dataset_path)
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

result = query_06(dataset_path)
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

result = query_07(dataset_path)
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
                   .merge(n2, left_on="s_nationkey", right_on="n_nationkey") \
                   .query('o_orderdate >= "1995-01-01" and o_orderdate <= "1996-12-31"') \
                   .query('p_type == "ECONOMY ANODIZED STEEL"')

    result = result.assign(
        o_year=pd.to_datetime(result['o_orderdate']).dt.year,
        volume=result['l_extendedprice'] * (1 - result['l_discount']),
        _tmp=np.where(result['n_name'] == "BRAZIL", result['volume'], 0)
    )

    result = result.groupby('o_year') \
                  .agg(mkt_share=('_tmp', 'sum') / ('volume', 'sum')) \
                  .round(2) \
                  .reset_index() \
                  .sort_values('o_year')

    return result
result = query_08(dataset_path)
print(f"Pandas Query 08 : \n{result}")
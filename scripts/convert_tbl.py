import os
import polars as pl
from pathlib import Path

# Lê a variável de ambiente SCALE_FACTOR, default = 1
scale_factor = os.getenv("SCALE_FACTOR", "1")

# Pasta onde estão os .tbl
tbl_dir = Path(f"data_tbl/scale-{scale_factor}")

if not tbl_dir.exists():
    raise FileNotFoundError(f"Pasta {tbl_dir} não existe. Gere os .tbl antes!")

# Define as colunas para cada tabela
table_columns = {
    "customer": [
        "c_custkey","c_name","c_address","c_nationkey","c_phone",
        "c_acctbal","c_mktsegment","c_comment"
    ],
    "lineitem": [
        "l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity",
        "l_extendedprice","l_discount","l_tax","l_returnflag","l_linestatus",
        "l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode",
        "comments"
    ],
    "nation": [
        "n_nationkey","n_name","n_regionkey","n_comment"
    ],
    "orders": [
        "o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate",
        "o_orderpriority","o_clerk","o_shippriority","o_comment"
    ],
    "part": [
        "p_partkey","p_name","p_mfgr","p_brand","p_type",
        "p_size","p_container","p_retailprice","p_comment"
    ],
    "partsupp": [
        "ps_partkey","ps_suppkey","ps_availqty","ps_supplycost","ps_comment"
    ],
    "region": [
        "r_regionkey","r_name","r_comment"
    ],
    "supplier": [
        "s_suppkey","s_name","s_address","s_nationkey","s_phone",
        "s_acctbal","s_comment"
    ],
}

# Loop para cada tabela
for table_name, columns in table_columns.items():
    print(f"Processando tabela: {table_name}")
    
    path = tbl_dir / f"{table_name}.tbl"

    if not path.exists():
        print(f"Atenção: {path} não existe, pulando.")
        continue

    # Lê o .tbl com Polars
    lf = pl.scan_csv(
        path,
        has_header=False,
        separator="|",
        try_parse_dates=True,
        new_columns=columns
    )

    # Remove a última coluna vazia (se existir)
    lf = lf.select(columns)

    # Salva CSV e Parquet na mesma pasta da escala
    lf.sink_parquet(tbl_dir / f"{table_name}.parquet")
    lf.sink_csv(tbl_dir / f"{table_name}.csv")

print(f"Conversão concluída em {tbl_dir}.")

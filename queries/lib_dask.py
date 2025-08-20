from datetime import datetime, timedelta
import dask.dataframe as dd
import os

scale = os.environ.get("SCALE_FACTOR", "1")
dataset_path = f"data_tbl/scale-{scale}/"
fs = None


def query_01(dataset_path, fs, scale):
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


result = query_01(dataset_path, fs, scale)
print(result.compute())
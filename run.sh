#!/bin/bash
# run.sh

# Define Scale Factor (ou usa valor já exportado)
SCALE_FACTOR=1

#Define número de núcleos (ou usa valor já exportado)
N_CORES=1

export N_CORES
export SCALE_FACTOR

# Cria o ambiente virtual e instala dependências
#./scripts/create_env.sh

# Ativa o ambiente virtual
source .venv/bin/activate

# Gera os .tbl
#./scripts/gen_data.sh

# Converte para CSV e Parquet usando Polars
#python3 scripts/convert_tbl.py

python3 -m queries.lib_dask

#!/bin/bash
# run.sh

WORK_DIR=$(pwd)

# Define Scale Factor (ou usa valor já exportado)
SCALE_FACTOR=1

#Define número de núcleos (ou usa valor já exportado)
N_CORES=1

#Define Modin engine dask,ray ou python.
MODIN_ENGINE=python
STORAGE_FORMAT=pandas
#MODIN_MEMORY=8000000000  # 8GB
#MODIN_ENGINE_MEMORY=4000000000

export N_CORES
export SCALE_FACTOR
export MODIN_ENGINE
export STORAGE_FORMAT
#export MODIN_MEMORY
#export MODIN_ENGINE_MEMORY
export WORK_DIR

# Cria o ambiente virtual e instala dependências
./scripts/create_env.sh

# Ativa o ambiente virtual
source .venv/bin/activate

# Gera os .tbl
./scripts/gen_data.sh

# Converte para CSV e Parquet usando Polars
python3 scripts/convert_tbl.py

python3 -m queries.lib_dask
python3 -m queries.lib_pandas
python3 -m queries.lib_modin
python3 -m queries.lib_polars
python3 -m queries.lib_pyspark

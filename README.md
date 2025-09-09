[![wakatime](https://wakatime.com/badge/user/939444ec-3797-43ff-bb09-6e41081ae12c/project/b530f65f-16ef-4154-9773-b2535def8ce3.svg)](https://wakatime.com/badge/user/939444ec-3797-43ff-bb09-6e41081ae12c/project/b530f65f-16ef-4154-9773-b2535def8ce3)

# TPC-H 3.0 Setup e Execução

Este repositório contém scripts e instruções para gerar os dados do benchmark TPC-H 3.0, criar o ambiente Python necessário e converter os arquivos gerados para CSV e Parquet.

---

## Pré-requisitos

Antes de iniciar, certifique-se de ter instalado:

- Python 3.11 ou superior
- `pip`
- Ferramentas de compilação (`build-essential`) no Linux
- Git (opcional, se estiver clonando o repositório)

No Ubuntu/Debian:

```bash
sudo apt update
sudo apt install build-essential
```
---

## Configuração do TPC-H

1 - Entre na pasta dbgen:
```bash
cd dbgen/
```
2 - Edite o arquivo makefile.suite para os seguintes parâmetros:

```bash
# Compiler
CC = gcc
# Database, machine, workload
DATABASE = NONE       # apenas para compilação do dbgen, não importa
MACHINE = LINUX
WORKLOAD = TPCH
```

3 - Compile o dbgen:
```bash
make -f makefile.suite
cd ..
```
4 - Alterar permissões dos arquivos
```bash
chmod +x run.sh scripts/gen_data.sh
./run.sh

```

5-Observações
- Se o Polars não estiver instalado automaticamente, ative a env manualmente e instale:
```bash
source .venv/bin/activate
pip install polars
```

- Para alterar o Scale Factor, exporte a variável antes de rodar o run.sh:

```bash
export SCALE_FACTOR=5
./run.sh

```
---
### Variável: `N_CORES`
---
Controla o número de núcleos de CPU utilizados pelas bibliotecas paralelas.

**Valores possíveis:**
- `N_CORES=1`: Execução single-thread (padrão)
- `N_CORES=4`: Utiliza 4 núcleos para processamento paralelo  
- `N_CORES=8`: Utiliza 8 núcleos para máxima paralelização

## Configuração do Modin

### Variáveis Principais:

#### `MODIN_ENGINE` - Motor de Execução
```bash
export MODIN_ENGINE=python    # Single-thread (padrão)
export MODIN_ENGINE=dask      # Paralelismo com Dask
export MODIN_ENGINE=ray       # Paralelismo com Ray
```

### Variáveis de Controle de Memória:

#### `MODIN_MEMORY` - Limite Total de Memória
```bash
export MODIN_MEMORY=8000000000  # 8GB em bytes
export MODIN_ENGINE_MEMORY=4000000000  # 4GB por worker
```

### Variável para controle do Dataframe principal
```bash
export STORAGE_FORMAT=pandas   # DataFrame pandas tradicional
export STORAGE_FORMAT=omnisci  # Requer OmniSciDB
export STORAGE_FORMAT=hdk      # Requer Intel Analytics Zoo
```
---
### Referências

http://www.tpc.org/tpch/

---
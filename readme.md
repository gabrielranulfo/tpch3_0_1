# TPC-H 3.0 Setup e Execução

Este repositório contém scripts e instruções para gerar os dados do benchmark TPC-H 3.0, criar o ambiente Python necessário e converter os arquivos gerados para CSV e Parquet.

---

## Pré-requisitos

Antes de iniciar, certifique-se de ter instalado:

- Python 3.12 ou superior
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
### Referências

http://www.tpc.org/tpch/

---
#!/bin/bash
# Script para gerar dados do TPC-H usando dbgen
# Rodar a partir da raiz do projeto

SCALE_FACTOR=${SCALE_FACTOR:-1}  # Default scale factor = 1
OUTDIR="data_tbl"
SCALE_DIR="scale-${SCALE_FACTOR}"

# Cria a pasta de destino específica para a escala
mkdir -p "$OUTDIR/$SCALE_DIR"

# Vai para a pasta dbgen, compila e gera os arquivos
cd dbgen || { echo "Erro: não foi possível acessar dbgen"; exit 1; }

echo "Compilando dbgen..."
make -f makefile.suite dbgen

echo "Gerando arquivos .tbl com scale factor = $SCALE_FACTOR ..."
./dbgen -s -y "$SCALE_FACTOR"

# Move os arquivos para a pasta de saída específica
mv *.tbl "../$OUTDIR/$SCALE_DIR"/

echo "Concluído. Arquivos .tbl gerados em $OUTDIR/$SCALE_DIR."

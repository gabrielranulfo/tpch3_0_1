#!/bin/bash
# create_env.sh
# Cria o ambiente virtual e instala dependências

VENV_DIR=".venv"

# Cria o ambiente virtual se não existir
if [ ! -d "$VENV_DIR" ]; then
    echo "Criando ambiente virtual em $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
else
    echo "Ambiente virtual já existe em $VENV_DIR"
fi

# Ativa o ambiente
source "$VENV_DIR/bin/activate"

# Atualiza pip
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install polars
#"$VENV_DIR/bin/pip" install polars-lts-cpu

"$VENV_DIR/bin/pip" install -r requirements.txt

# Baixa e extrai Spark e OpenJDK

wget -q wget https://dlcdn.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz
wget -q https://download.java.net/openjdk/jdk17.0.0.1/ri/openjdk-17.0.0.1+2_linux-x64_bin.tar.gz

tar xf spark-4.0.1-bin-hadoop3.tgz
tar xf openjdk-17.0.0.1+2_linux-x64_bin.tar.gz
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
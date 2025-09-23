FROM python:3.13-slim

# Atualizar pacotes do sistema para corrigir vulnerabilidades
RUN apt-get update && apt-get upgrade -y && apt-get install -y git bash && rm -rf /var/lib/apt/lists/*

# Clonar repositório
RUN git clone https://github.com/gabrielranulfo/tpch3_0_1.git /workspace

# Entrar no repositório
WORKDIR /workspace

# Garantir permissão de execução e rodar o script
RUN chmod +x run.sh

RUN ./run.sh

# Atualizar pip e instalar dependências
RUN pip install --upgrade pip

# Mantém container ativo (caso queira usá-lo como dev)
CMD ["tail", "-f", "/dev/null"]
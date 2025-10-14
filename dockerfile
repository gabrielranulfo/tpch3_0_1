ARG CACHE_BUST=1

FROM python:3.13-slim

# Atualizar pacotes do sistema para corrigir vulnerabilidades
RUN apt-get update && apt-get upgrade -y && apt-get install -y git bash tar wget

RUN apt-get update && apt-get install -y nano vim htop

RUN apt-get update && apt-get install -y openjdk-17-jdk

RUN mkdir /workspace

# Clonar repositório
RUN git clone https://github.com/gabrielranulfo/tpch3_0_1.git /workspace

# Entrar no repositório
WORKDIR /workspace

RUN git checkout docker_config

# Executar os comandos do create_env.sh diretamente no Dockerfile
RUN VENV_DIR=".venv" && \
    # Cria o ambiente virtual se não existir
    if [ ! -d "$VENV_DIR" ]; then \
        echo "Criando ambiente virtual em $VENV_DIR..." && \
        python3 -m venv "$VENV_DIR"; \
    else \
        echo "Ambiente virtual já existe em $VENV_DIR"; \
    fi

# Atualiza pip e instala dependências (usando o venv)
RUN ./.venv/bin/pip install --upgrade pip && \
    ./.venv/bin/pip install polars && \
    ./.venv/bin/pip install -r requirements.txt

# Baixa e extrai Spark e OpenJDK
#RUN wget -q https://dlcdn.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz && \
#    wget -q https://download.java.net/openjdk/jdk17.0.0.1/ri/openjdk-17.0.0.1+2_linux-x64_bin.tar.gz && \
#    tar xf spark-4.0.1-bin-hadoop3.tgz && \
#    tar xf openjdk-17.0.0.1+2_linux-x64_bin.tar.gz

# Configurar environment variables para Java e Spark
#ENV JAVA_HOME=/workspace/jdk-17.0.0.1+2
#ENV PATH=$JAVA_HOME/bin:$PATH
#ENV SPARK_HOME=/workspace/spark-4.0.1-bin-hadoop3
#ENV PATH=$SPARK_HOME/bin:$PATH

# Garantir permissão de execução e rodar o script usando o venv

RUN chmod +x /workspace/*.sh
RUN /workspace/run.sh

# Mantém container ativo (caso queira usá-lo como dev)
CMD ["tail", "-f", "/dev/null"]
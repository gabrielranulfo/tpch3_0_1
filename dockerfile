ARG CACHE_BUST=1

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Instalar tudo em uma única camada para imagem menor
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    git bash tar wget nano vim htop \
    curl ca-certificates software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    python3.13 python3.13-venv python3.13-dev python3-pip \
    openjdk-17-jdk && \
    ln -sf /usr/bin/python3.13 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.13 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip && \
    rm -rf /var/lib/apt/lists/*

# Verificar instalações
RUN java -version && python --version

RUN mkdir /workspace && \
    git clone https://github.com/gabrielranulfo/tpch3_0_1.git /workspace

WORKDIR /workspace

RUN git checkout docker_config

# Ambiente virtual e dependências
RUN python3.13 -m venv .venv && \
    ./.venv/bin/pip install --upgrade pip polars && \
    ./.venv/bin/pip install -r requirements.txt && \
    chmod +x *.sh

CMD ["tail", "-f", "/dev/null"]
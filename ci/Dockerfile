FROM ubuntu:17.10

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    cmake \
    gcc \
    libc6-dev \
    make \
    pkg-config \
    libclang-dev \
    clang \
    libclang-dev \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    build-essential \
    python \
    g++ \
    gcc

COPY xargo.sh /
RUN bash /xargo.sh

COPY openssl.sh /
RUN apt-get install -y --no-install-recommends \
    g++ \
    zlib1g-dev && \
    bash /openssl.sh linux-x86_64

ENV OPENSSL_DIR=/openssl \
    OPENSSL_INCLUDE_DIR=/openssl/include \
    OPENSSL_LIB_DIR=/openssl/lib


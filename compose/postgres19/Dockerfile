FROM debian:bookworm-slim@sha256:c67e92c8099ba22853ab4cfda7cfb494d253d6012ca82dee98ba8976772af018 AS builder

ARG PG_VERSION=19.0
ARG PG_SHA256=a89fd70efbad6a08c7dbed8d4872e3af30170d9e5ca9dc7906523be95b4e692d

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bison \
        build-essential \
        bzip2 \
        ca-certificates \
        curl \
        flex \
        libicu-dev \
        liblz4-dev \
        libreadline-dev \
        libssl-dev \
        libzstd-dev \
        pkg-config \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src
RUN curl -fsSL "https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.bz2" -o "postgresql-${PG_VERSION}.tar.bz2" \
    && echo "${PG_SHA256}  postgresql-${PG_VERSION}.tar.bz2" | sha256sum -c - \
    && tar -xjf "postgresql-${PG_VERSION}.tar.bz2" \
    && rm "postgresql-${PG_VERSION}.tar.bz2"

WORKDIR /usr/src/postgresql-19.0
RUN ./configure \
        --prefix=/usr/local/pgsql \
        --with-icu \
        --with-openssl \
        --with-lz4 \
        --with-zstd \
    && make -j"$(nproc)" world-bin \
    && make install-world-bin

FROM debian:bookworm-slim@sha256:c67e92c8099ba22853ab4cfda7cfb494d253d6012ca82dee98ba8976772af018 AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        gosu \
        libicu72 \
        liblz4-1 \
        libreadline8 \
        libssl3 \
        libzstd1 \
        locales \
        zlib1g \
    && sed -i 's/^# *en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
    && locale-gen \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r -g 999 postgres \
    && useradd -r -u 999 -g postgres -d /var/lib/postgresql -s /bin/bash postgres \
    && mkdir -p /var/lib/postgresql/19/data /var/run/postgresql \
    && chown -R postgres:postgres /var/lib/postgresql /var/run/postgresql

COPY --from=builder /usr/local/pgsql /usr/local/pgsql
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/docker-entrypoint.sh \
    && chmod +x /usr/local/bin/docker-entrypoint.sh

ENV PATH="/usr/local/pgsql/bin:${PATH}"
ENV PGDATA="/var/lib/postgresql/19/data"
ENV LANG="en_US.UTF-8"

EXPOSE 5432
STOPSIGNAL SIGINT

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD []

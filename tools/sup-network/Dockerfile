FROM ubuntu:18.10

COPY scripts/* /usr/bin

RUN apt-get update

RUN apt-get install -y \
            libarchive-dev \
            libczmq-dev \
            libsodium-dev \
            libssl1.0-dev

RUN apt-get install -y \
            jq \
            curl

RUN useradd --user-group hab

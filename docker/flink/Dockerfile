FROM flink:1.18.1-scala_2.12-java11

ARG TARGETARCH

USER root

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-${TARGETARCH}
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        openjdk-11-jdk-headless \
        python-is-python3 \
        python3 \
        python3-dev \
        python3-pip \
    && export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" \
    && pip3 install --no-cache-dir apache-flink==1.18.1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER flink

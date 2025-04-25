ARG base_image=ubuntu:24.04

FROM ${base_image}

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -u -y install lsb-release python3 binutils python3-venv python3-pip wget git bash-completion vim yq jq \
    && apt-get -qq purge && apt-get -qq clean && rm -rf /var/lib/apt/lists/*

COPY /cicd/docker/requirements.txt /
RUN pip install --no-cache-dir --break-system-packages --ignore-requires-python --use-pep517 -r /requirements.txt

COPY ./setup.py ./README.md ./MANIFEST.in /
COPY ./janus /janus
COPY ./tests /tests
RUN python3 -m pip install --break-system-packages --no-cache-dir --use-pep517 .

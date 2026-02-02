ARG base_image=ubuntu:24.04

FROM ${base_image}

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -u -y install lsb-release python3 binutils python3-venv python3-pip wget curl git bash-completion vim yq jq \
    && apt-get -qq purge && apt-get -qq clean && rm -rf /var/lib/apt/lists/* \
    && curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl \
    && chmod +x kubectl && mv kubectl /usr/local/bin/

COPY /cicd/docker/requirements.txt /
RUN pip install --no-cache-dir --break-system-packages --ignore-requires-python --use-pep517 -r /requirements.txt \
   && pip install --no-cache-dir --break-system-packages --ignore-requires-python --use-pep517 coverage

COPY ./setup.py ./README.md ./MANIFEST.in /
COPY ./janus /janus
COPY ./tests /tests
RUN python3 -m pip install --break-system-packages --no-cache-dir --use-pep517 .

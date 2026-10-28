ARG base_image=ubuntu:24.04

FROM ${base_image}

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -u -y install lsb-release python3 binutils python3-venv python3-pip wget curl git bash-completion vim yq jq \
    && apt-get -qq purge && apt-get -qq clean && rm -rf /var/lib/apt/lists/* \
    && curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl \
    && chmod +x kubectl && mv kubectl /usr/local/bin/

# Copy project files
COPY ./requirements.txt /requirements.txt
COPY ./pyproject.toml ./README.md ./MANIFEST.in /
COPY ./janus /janus
COPY ./tests /tests

# Install dependencies first, then install the package without build isolation
RUN pip install --break-system-packages --no-cache-dir -r /requirements.txt \
    && pip install --break-system-packages --no-cache-dir --no-build-isolation ".[swagger,scalar]"

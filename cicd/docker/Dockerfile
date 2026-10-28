ARG base_image=ubuntu:24.04

FROM ${base_image}

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -u -y install lsb-release python3 binutils python3-venv python3-pip wget curl git bash-completion vim yq jq \
    && apt-get -qq purge && apt-get -qq clean && rm -rf /var/lib/apt/lists/* \
    && curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl \
    && chmod +x kubectl && mv kubectl /usr/local/bin/

# Create a virtual environment to avoid system Python conflicts
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy project files
COPY ./pyproject.toml ./README.md ./MANIFEST.in /
COPY ./janus /janus
COPY ./tests /tests

# Install the project and all dependencies
RUN pip install --no-cache-dir ".[swagger,scalar]"

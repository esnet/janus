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

# Pre-install all dependencies explicitly (avoids pyproject.toml build isolation hang)
RUN pip install --no-cache-dir \
    cryptography \
    "Flask>=3.0.2" \
    "flask-openapi3[swagger,scalar]>=4.3.1" \
    "Flask-httpauth>=4.8.0" \
    "flask-sock>=0.7.0" \
    "PyJWT>=2.10.1" \
    "Flask-JWT-Extended>=4.7.1" \
    portainer-api \
    psutil \
    py-cpuinfo \
    pyyaml \
    requests \
    six \
    tinydb \
    virtfs \
    tcconfig \
    "Werkzeug>=3.0.1" \
    "pydantic>=2.12.0" \
    "ansible-tower-cli==3.3.8" \
    pytest-xprocess \
    "kubernetes>=29.0.0" \
    openapi-slurm-0.0.38-client \
    "sense-o-api==1.37" \
    pytest-cov \
    coverage

# Copy project files
COPY ./pyproject.toml ./README.md ./MANIFEST.in /
COPY ./janus /janus
COPY ./tests /tests

# Install janus package without build isolation (deps already installed above)
RUN pip install --no-cache-dir --no-build-isolation ".[swagger,scalar]"

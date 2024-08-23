# Janus Controller

A container (Portainer Docker) controller with profiles for common Data Transfer
Node (DTN) capabilities. Support DTN-as-a-Service deployments.

## Build Instructions
```
python -m build
```
Upload to PyPi using
```
twine upload dist/*
```

## Install Instructions
```
git clone https://github.com/esnet/janus.git
cd janus
pip3 install -e .
```

### Development quick-start

Without installing any of the dependencies locally, you can use an existing
Docker compose stack to run the controller and supporting services.
From within the clone Janus repository:

```
cd janus/config
cp janus.conf.example janus.conf   <-- edit as needed

cd ../scripts
docker compose -f local-dev-compose.yml up -d
```

This will start the Janus controller in a development context along
with supporting container images.  The janus/config directory along
with the relative source tree will be mounted inside the container.

```
volumes:
 - ./..:/opt/janus
 - ./../janus/config:/etc/janus
```

Edits to the source code will invoke a controller reload. Additional
configuration files can be placed in the config/ subdirectory as
needed, for example a kubecfg.

### Adding Kubernetes config

Bind mount your K8s `config` file into the $HOME of the Janus controller container.
For example:

```
volumes:
  - ./../janus/config/nrp.config:/home/janus/.kube/config
```

Then, when you access the Janus Web interface Endpoints view, the controller should
list the resources queried from all the contexts in your K8s config.

# Configuring container registry authentication

The Janus controller supports authentication to private container
registries using tokens passed via the X-Registry-Auth HTTP
header. The tokens are in the form of a base64 encoded dictionary
containing the following attributes:

```
{ "username": "",
  "password": "",
  "serveraddress": ""
}
```

As an example, Harbor registries allow for the creation of robot
accounts with secret keys. Using one of these robot accounts, a valid
token for Janus/Portainer can be created as follows:

```
echo '{"username": "robot+dtnaas+deployer", "password": "SECRET_KEY", "serveraddress": "wharf.es.net"}' | base64 -w 0
```

For a single authenticated registry, this token can be passed as an
environment variable when launching the controller process. In a Janus
controller Docker compose file, include the following:

```
   ...
   environment:
      - REGISTRY_AUTH=<TOKEN>
   ...
```

Within the Janus `settings.py` file is where the registry auth
dictionary is maintained to map registry servers to authentication
tokens. Additional registries with their associated auth tokens may be
defined as needed.

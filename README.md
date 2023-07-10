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

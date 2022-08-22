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

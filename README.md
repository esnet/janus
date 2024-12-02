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

## Project Setup
This guide provides step-by-step instructions for setting up the Janus Controller environment.

### Prerequisites
- Docker
- Docker Compose

### Setup Instructions
- Copy Configuration Files:
    ```
    sudo mkdir -p /etc/janus
    sudo cp janus/config/* /etc/janus/
    cp janus.conf.example janus.conf   <-- edit as needed
    cd ../scripts
    docker compose -f local-dev-compose.yml up -d
   ``` 
   - Set the Portainer user and password in the janus.conf file.


- Create the db.json File and Set Permissions:
    ```
    sudo touch /etc/janus/db.json
    sudo chown 1000:1000 /etc/janus/db.json
   ```
   
- Create a Docker Volume:
    ```
    docker volume create portainer_data
   ```
   
- Update and Run Docker Compose:
    - Navigate to the /scripts directory in the cloned repository:
    ```
   cd /path/to/janus/scripts
   ```
    - Edit the local-dev-compose.yml file to update the controller’s volume path to the location of the code on your local machine.
   ```
    volumes:
      - /path/to/local/janus:/opt/janus
   ```
    - Edits to the source code will invoke a controller reload. Additional
configuration files can be placed in the config/ subdirectory as
needed, for example a kubecfg.

  
- Run the Docker Compose file:
  ```
   docker-compose -f local-dev-compose.yml up -d
   ```
  - This will start the Janus controller in a development context along
with supporting container images.  The janus/config directory along
with the relative source tree will be mounted inside the container. 

   
- Access Portainer:
   - Open a web browser and navigate to https://localhost:<port#> (replace <port#> with the actual port number specified in the janus.conf file).
   - Log in using the credentials you set earlier.


### Adding Kubernetes config

   - Bind mount your K8s `config` file into the $HOME of the Janus controller container.
For example:

     ```
      volumes:
        - ./../janus/config/nrp.config:/home/janus/.kube/config
     ```

   - Then, when you access the Janus Web interface Endpoints view, the controller should
list the resources queried from all the contexts in your K8s config.


### Configuring container registry authentication

  - The Janus controller supports authentication to private container
registries using tokens passed via the X-Registry-Auth HTTP
header. The tokens are in the form of a base64 encoded dictionary
containing the following attributes:

    ```
    { "username": "",
      "password": "",
      "serveraddress": ""
    }
    ```

  - As an example, Harbor registries allow for the creation of robot
accounts with secret keys. Using one of these robot accounts, a valid
token for Janus/Portainer can be created as follows:

    ```
    echo '{"username": "robot+dtnaas+deployer", "password": "SECRET_KEY", "serveraddress": "wharf.es.net"}' | base64 -w 0
    ```

  - For a single authenticated registry, this token can be passed as an
environment variable when launching the controller process. In a Janus
controller Docker compose file, include the following:

    ```
    environment:
      - REGISTRY_AUTH=<TOKEN>
    ```


  - Within the Janus `settings.py` file is where the registry auth
dictionary is maintained to map registry servers to authentication
tokens. Additional registries with their associated auth tokens may be
defined as needed.

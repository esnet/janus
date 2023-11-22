import logging
import time
import requests
from threading import Thread
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor
from tinydb import TinyDB, Query
from janus import settings
from janus.settings import cfg
from janus.api.utils import handle_image
from janus.api.portainer import PortainerDockerApi
from portainer_api.rest import ApiException


log = logging.getLogger(__name__)

class AgentMonitor(object):
    def __init__(self, client=None):
        self._th = None
        self._stop = False
        self._client = client
        self._dapi = PortainerDockerApi(client)
        log.debug("Initialized Janus Agent Monitor")

    def start(self):
        self._th = Thread(target=self._thr, args=())
        self._th.start()

    def stop(self):
        self._stop = True
        self._th.join()

    def start_agent(self, n):
        log.info(f"Attempting to start agent {n['name']}")
        img = settings.AGENT_IMAGE
        docker_kwargs = {
            "HostConfig": {
                "RestartPolicy": {"Name": "unless-stopped"},
                "NetworkMode": "host",
                "Privileged": True
            },
            "Tty": True,
            "Env": [f"AGENT_PORT={settings.AGENT_PORT}"]
        }
        try:
            ret = self._dapi.get_containers(n['id'])
            for i in ret:
                if i['Image'] == settings.AGENT_IMAGE:
                    log.info(f"Agent container is already running on {n['name']}, check firewall?")
                    return
            handle_image(n, img, self._dapi)
            ret = self._dapi.create_container(n['id'], img, **docker_kwargs)
            self._dapi.start_container(n['id'], ret['Id'])
        except ApiException as e:
            log.error(f"Could not start agent container on {n['name']}: {e.reason}: {e.body}")

    def check_agent(self, n, url=None):
        url = url if url else n['public_url']
        try:
            ret = requests.get("{}://{}:{}/api/janus/agent/node".format(settings.AGENT_PROTO,
                                                                        url,
                                                                        settings.AGENT_PORT),
                               verify=settings.AGENT_SSL_VERIFY,
                               timeout=2)
            return n, ret
        except Exception as e:
            raise e

    def tune(self, n, url=None, post=False):
        url = url if url else n['public_url']
        if post:
            fn = requests.post
            log.debug("Applying agent tuning at {}".format(url))
        else:
            fn = requests.get
        try:
            ret = fn("{}://{}:{}/api/janus/agent/tune".format(settings.AGENT_PROTO,
                                                              url,
                                                              settings.AGENT_PORT),
                     verify=settings.AGENT_SSL_VERIFY,
                     timeout=2,
                     auth=(settings.AGENT_USERNAME, settings.AGENT_PASSWORD))
            return n, ret
        except Exception as e:
            raise e

    def _thr(self):
        while not self._stop:
            log.info("Checking on agent status")
            DB = TinyDB(cfg.get_dbpath())
            Node = Query()
            nodes = DB.table('nodes').all()
            futures = list()
            with ThreadPoolExecutor(max_workers=8) as executor:
                for n in nodes:
                    futures.append(executor.submit(self.check_agent, n))
            for future in concurrent.futures.as_completed(futures):
                item,ret = future.result()
                if not ret:
                    self.start_agent(item)
            time.sleep(10)

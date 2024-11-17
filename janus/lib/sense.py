import logging
import time
import requests
from threading import Thread
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor
from janus.settings import JanusConfig
from sense.client.metadata_api import MetadataApi
from sense.common import getConfig

log = logging.getLogger(__name__)


class SENSEMetaManager(object):
    def __init__(self, cfg: JanusConfig):
        self._cfg = cfg
        self._stop = False
        self._interval = 5
        try:
            self._client = MetadataApi()
        except Exception as e:
            log.error(f"Error: {e}")
        log.debug(f"Initialized {__name__}")

    def start(self):
        self._th = Thread(target=self._run, args=())
        self._th.start()

    def stop(self):
        log.debug(f"Stopping {__name__}")
        self._stop = True
        self._th.join()

    def _run(self):
        cnt = 0
        while not self._stop:
            time.sleep(1)
            cnt += 1
            if cnt == self._interval:
                log.info("Synchronizing SENSE-O Metadata ")
                try:
                    res = self._client.get_metadata(domain="JANUS", name="overview")
                    print (res)
                    nodes = self._cfg.sm.get_nodes()
                    print (nodes)
                except Exception as e:
                    log.error(f"Could not retrieve metadata: {e}")
                cnt = 0

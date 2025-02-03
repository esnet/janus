import json
import logging

from sense.client.metadata_api import MetadataApi
from sense.client.requestwrapper import RequestWrapper
from sense.client.task_api import TaskApi

from janus.lib.sense_utils import SenseConstants

log = logging.getLogger(__name__)


class SENSEApiHandler:
    def __init__(self, url, req_wrapper=None):
        req_wrapper = req_wrapper or RequestWrapper()
        self.url = url
        self.task_client = TaskApi(req_wrapper=req_wrapper)
        self.metadata_client = MetadataApi(req_wrapper=req_wrapper)
        self.retries = SenseConstants.SENSE_PLUGIN_RETRIES

    def retrieve_tasks(self, assigned, status):
        err = None

        for attempt in range(self.retries):
            try:
                tasks = self.task_client.get_tasks_agent_status(assigned=assigned, status=status)

                if isinstance(tasks, list):
                    return tasks

                err = tasks
            except Exception as e:
                err = str(e)

            import time
            time.sleep(1)

        log.error(f'Giving up on retrieving tasks after {self.retries} attempts....{err}')
        return None

    def _update_task(self, data, **kwargs):
        err = None

        for attempt in range(self.retries):
            try:
                ret = self.task_client.update_task(json.dumps(data), **kwargs)

                if isinstance(ret, dict):
                    return True

                err = ret
            except Exception as e:
                err = str(e)

            import time
            time.sleep(1)

        log.error(f'Giving up on updating task after {self.retries} attempts:{kwargs}: error={err}')
        return False

    def accept_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets,  message=message)
        return self._update_task(data, uuid=uuid, state='ACCEPTED')

    def reject_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets, message=message)
        return self._update_task(data, uuid=uuid, state='REJECTED')

    def fail_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets, message=message)
        return self._update_task(data, uuid=uuid, state='FAILED')

    def wait_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets, message=message)
        return self._update_task(data, uuid=uuid, state='WAITING')

    def finish_task(self, uuid, targets, message):
        data = dict(url=self.url, targets=targets, message=message)
        return self._update_task(data, uuid=uuid, state='FINISHED')

    def get_metadata(self, domain, name):
        return self.metadata_client.get_metadata(domain=domain, name=name)

    def post_metadata(self, metadata, domain, name):
        err = None

        for attempt in range(self.retries):
            try:
                ret = self.metadata_client.post_metadata(data=json.dumps(metadata), domain=domain, name=name)

                if isinstance(ret, dict):
                    return True

                err = ret
            except Exception as e:
                err = str(e)

            import time
            time.sleep(1)

        log.error(f'Giving up on updating metadata after {self.retries} attempts: {err}')
        return None

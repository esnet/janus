import os

from tests.sense_runnable import SenseRunnable
from tests.sense_test_utils import get_logger

log = get_logger()


class ServerRunnable(SenseRunnable):
    def __init__(self, database, config_file, sense_api_handler=None, node_name_filter=None):
        super().__init__(database, config_file, sense_api_handler, node_name_filter)

    def run(self):
        while True:
            for plugin in self.mngr.cfg.plugins:
                plugin.run()


def run_server():
    DB_FILE_NAME = 'db-test-sense.json'
    JANUS_CONF_TEST_FILE = 'janus-sense-test.conf'

    runnable = ServerRunnable(
        database=os.path.join(os.getcwd(), DB_FILE_NAME),
        config_file=os.path.join(os.getcwd(), JANUS_CONF_TEST_FILE),
        node_name_filter=None
    )

    runnable.init()
    runnable.run()


if __name__ == '__main__':
    run_server()

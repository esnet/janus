import logging.config
import os

logging_conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '../janus/config/logging.conf'))
logging.config.fileConfig(logging_conf_path)
log = logging.getLogger(__name__)

DB_FILE_NAME = 'db-test-sense.json'
JANUS_CONF_TEST_FILE = 'janus-sense-test.conf'

FILTER_NODES = False

if FILTER_NODES:
    ENDPOINTS = ['k8s-gen5-01.sdsc.optiputer.net',
                 'k8s-gen5-02.sdsc.optiputer.net',
                 'losa4-nrp-01.cenic.net',
                 'k8s-3090-01.clemson.edu',
                 'node-2-8.sdsc.optiputer.net']
else:
    ENDPOINTS = None


def test_server():
    from tests.sense_runnable import SenseRunnable

    tsw = SenseRunnable(
        database=os.path.join(os.getcwd(), DB_FILE_NAME),
        config_file=os.path.join(os.getcwd(), JANUS_CONF_TEST_FILE),
        node_name_filter=ENDPOINTS
    )

    tsw.init()
    tsw.run()


def test_using_base_script():
    from tests.sense_runnable import SenseRunnable
    from tests.sense_test_utils import TaskGenerator, GeneratorDone
    from tests.sense_test_utils import BaseScript, FakeSENSEApiHandler
    task_generator = TaskGenerator(BaseScript())
    sense_api_handler = FakeSENSEApiHandler(task_generator)
    tsw = SenseRunnable(
        database=os.path.join(os.getcwd(), DB_FILE_NAME),
        config_file=os.path.join(os.getcwd(), JANUS_CONF_TEST_FILE),
        sense_api_handler=sense_api_handler,
        node_name_filter=ENDPOINTS
    )

    tsw.init()
    i = 1

    try:
        while True:
            tsw.run()
            print(f"RUN ENDED: #{i}")
            i += 1
    except GeneratorDone:
        pass

    import json

    print(json.dumps(sense_api_handler.task_state_map, indent=2))


def test_using_complex_script():
    from tests.sense_runnable import SenseRunnable
    from tests.sense_test_utils import TaskGenerator, GeneratorDone
    from tests.sense_test_utils import ComplexScript, FakeSENSEApiHandler

    task_generator = TaskGenerator(ComplexScript())
    sense_api_handler = FakeSENSEApiHandler(task_generator)
    tsw = SenseRunnable(
        database=os.path.join(os.getcwd(), DB_FILE_NAME),
        config_file=os.path.join(os.getcwd(), JANUS_CONF_TEST_FILE),
        sense_api_handler=sense_api_handler,
        node_name_filter=ENDPOINTS
    )

    tsw.init()
    i = 1

    try:
        while True:
            tsw.run()
            print(f"RUN ENDED: #{i}")
            i += 1
    except GeneratorDone:
        pass

    import json

    print(json.dumps(sense_api_handler.task_state_map, indent=2))


if __name__ == '__main__':
    # test_server()
    test_using_base_script()
    test_using_complex_script()

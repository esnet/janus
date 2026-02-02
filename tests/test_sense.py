from tests.sense_test_utils import get_logger, get_db_file_path, get_janus_conf_file_path

log = get_logger()


def run_sense_task_workflow(script):
    from tests.sense_runnable import SenseRunnable
    from tests.sense_test_utils import TaskGenerator, GeneratorDone
    from tests.sense_test_utils import FakeSENSEApiHandler

    task_generator = TaskGenerator(script)
    sense_api_handler = FakeSENSEApiHandler(task_generator)
    tsw = SenseRunnable(
        database=get_db_file_path(),
        config_file=get_janus_conf_file_path(),
        sense_api_handler=sense_api_handler,
        node_name_filter=None
    )

    tsw.init()
    i = 1

    try:
        while True:
            tsw.run()
            log.debug(f"{script.prefix}:RUN ENDED: #{i}")
            i += 1
    except GeneratorDone:
        pass

    import json

    log.info(f'{script.prefix}: {json.dumps(sense_api_handler.task_state_map, indent=2)}')


def test_simple_task_script():
    from tests.sense_test_utils import SimpleScript

    run_sense_task_workflow(SimpleScript("simple"))


def test_using_basic_script():
    from tests.sense_test_utils import BaseScript

    run_sense_task_workflow(BaseScript("base"))


def test_using_complex_script():
    from tests.sense_test_utils import ComplexScript

    run_sense_task_workflow(ComplexScript("complex"))


if __name__ == '__main__':
    test_simple_task_script()
    test_using_basic_script()
    test_using_complex_script()

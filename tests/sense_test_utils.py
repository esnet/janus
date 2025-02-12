import logging

from janus.lib.sense_api_handler import SENSEApiHandler

log = logging.getLogger(__name__)


class GeneratorDone(Exception):
    pass


class BaseScript:
    def __init__(self):
        self.context = {"alias": "fake", "uuid": "base"}
        self.principals = ["aessiari@lbl.gov"]

        # self.nodes = ['k8s-gen5-01.sdsc.optiputer.net', 'k8s-gen5-02.sdsc.optiputer.net']
        self.nodes = ['sandie-3.ultralight.org', 'sandie-5.ultralight.org']
        self.ips = ['10.251.88.241/28', '10.251.88.242/28']

    def script(self):
        tasks = list()
        tasks.append(self.ptask1(3910))  # target1
        tasks.append(self.ptask2(3910))  # target2 with same vlan
        tasks.append(self.terminate_task())
        return tasks

    @staticmethod
    def _create_target(name, vlan, ip, principals):
        target = {
            "name": name,
            "vlan": vlan,
            "ip": ip,
            "principals": principals
        }

        return target

    def _create_template(self, uuid, command):
        template = {
            'config': {
                "command": command,
                "targets": [],
                "context": self.context
            },
            'uuid': uuid
        }

        return template

    def ptask1(self, vlan) -> list:
        task = self._create_template("ptask1", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan, self.ips[0], ['admin'])
        ]
        return [task]

    def ptask2(self, vlan):
        task = self._create_template("ptask2", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[1], vlan, None, self.principals)
        ]

        return [task]

    def terminate_task(self):
        task = self._create_template("ptask_terminate", "instance-termination-notice")
        return [task]


class ComplexScript(BaseScript):
    def __init__(self):
        super().__init__()
        self.context = {"alias": "fake", "uuid": "cplex"}

    def script(self):
        tasks = list()
        tasks.append(self.ptask1(3910))  # target1
        tasks.append(self.ptask2(3910))  # target2 with same vlan
        tasks.append(self.ptask1(3910))  # REPLAY

        tasks.append(self.ptask1(3912))  # two vlans
        tasks.append(self.ptask1(3912))  # replay

        tasks.append(self.ptask1(3909))
        tasks.append(self.ptask2(3909))  # back to one vlan
        tasks.append(self.ptask2(3909))  # REPLAY

        tasks.append(self.ptask3(3915, 3916))  # NEW VLANS AND IPS

        tasks.append(self.ptask4(3915, 3916))  # NO IPS

        tasks.append(self.terminate_task())
        return tasks

    def ptask3(self, vlan1, vlan2):
        task = self._create_template("ptask3", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan1, self.ips[0], self.principals),
            self._create_target(self.nodes[1], vlan2, self.ips[1], ['extra_user1'])
        ]

        return [task]

    def ptask4(self, vlan1, vlan2):
        task = self._create_template("ptask4", "handle-sense-instance")
        task['config']['targets'] = [
            self._create_target(self.nodes[0], vlan1, None, []),
            self._create_target(self.nodes[1], vlan2, None, ['extra_user2'])
        ]

        return [task]


class TaskGenerator:
    def __init__(self, script=None):
        script = script or BaseScript()
        self.tasks = script.script()

    def generate(self):
        for i in self.tasks:
            yield i

        raise GeneratorDone()


class FakeSENSEApiHandler(SENSEApiHandler):
    def __init__(self, gen):
        super().__init__('fake_url')
        self.gen = gen.generate()
        self.last_task = []
        self.task_state_map = dict()
        self.counter = 0

    def post_metadata(self, metadata, domain, name):
        return True

    def retrieve_tasks(self, assigned, status):
        try:
            for task in self.gen:
                self.last_task = task
                task[0]['uuid'] = str(self.counter) + '-' + task[0]['uuid']
                self.counter += 1
                return task
        except GeneratorDone as e:
            assert len(self.task_state_map) == self.counter
            raise e

    def _update_task(self, data, **kwargs):
        assert 'url' in data
        assert 'targets' in data
        assert 'message' in data
        assert 'uuid' in kwargs
        assert 'state' in kwargs

        if kwargs['uuid'] not in self.task_state_map:
            self.task_state_map[kwargs['uuid']] = kwargs['state']
        else:
            self.task_state_map[kwargs['uuid']] += ',' + kwargs['state']

        log.debug(f'faking updating task attempts:{data}:{kwargs}')
        return True

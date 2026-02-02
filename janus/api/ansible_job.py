# Copyright 2022, ESnet
#
#
from tower_cli import models, get_resource, resources, exceptions as exc
from tower_cli.api import client
from janus.settings import cfg
from janus.api.db import DBLayer

class AnsibleJob(models.ExeResource):
    """ An Ansible job.

    An Ansible Job is launched based on the given job_template.
    """
    endpoint = '/jobs'

    def launch(self, job_template=None, monitor=False, wait=False, timeout=None, extra_vars=None, limits=None, **kwargs):
        """ Launch a new job based on a job template.

        Creates a new job in Ansible Tower, starts it, and returns
        job ID for its status to be waited up to the timeout.

        :param job_template: Primary key or name of the job template to launch new job.
        :type job_template: str
        :param monitor: Flag that if set calls monitor on the newly launched job.
        :type monitor: bool
        :param wait: Flag if set, monitor the status of the job but do not print.
        :type wait: bool
        :param timeout: this attempt will time out after the given number of seconds.
        :type timeout: int
        :param extra_vars: yaml formatted texts that contains extra variables to pass on.
        :type extra_vars: array of strings
        :param limits: remote hosts
        :type limits: array of strings
        """
        # Get the job template from Ansible Tower.
        jt_resource = get_resource('job_template')
        jt = jt_resource.get(job_template)

        # Create the new job in Ansible Tower.
        start_data = {}
        endpoint = '/job_templates/%d/launch/' % jt['id']
        if extra_vars is None:
                    raise exc.UsageError(
                        'Extra variables not found. '
                        )
        else:
            start_data['extra_vars'] = extra_vars

        if limits is not None:
            start_data['limit'] = limits

        # Actually start the job.
        kwargs.update(start_data)
        job_started = client.post(endpoint, data=kwargs)

        # Get the job ID from the result.
        job_id = job_started.json()['id']

        # Get some information about the running job to print
        result = self.status(pk=job_id, detail=True)
        result['changed'] = True

        # monitor or wait for finishing till timeout
        if monitor:
            return self.monitor(job_id, timeout=timeout)
        elif wait:
            return self.wait(job_id, timeout=timeout)

        return result


if __name__ == '__main__':
        # jt_name = 'DTNaaS update routes'
        # ex_vars = '{"ipprot": "ipv4", "interface": "eth0", "gateway": "172.17.0.1", "container": "dtnaas-controller"}'
        # limit = 'lbl-dev-dtn.es.net'
        cfg.setdb()
        dbase = cfg.db
        cfg.pm.read_profiles(path="/etc/janus/profiles")
        prof = cfg.pm.get_profile('my-test-profile')
        for psname in prof['settings']['post_starts']:
            ps = cfg.get_poststart(psname)
            if ps['type'] == 'ansible':
                jt_name = ps['jobtemplate']
                gateway = ps['gateway']
                ipprot = ps['ipprot']
                inf = ps['interface']
                limit = ps['limit']
                container_name= ps['container_name']

                ex_vars = f'{{"ipprot": "{ipprot}", "interface": "{inf}", "gateway": "{gateway}", "container": "{container_name}"}}'
                job = AnsibleJob()
                try:
                    result = job.launch(job_template=jt_name, monitor=True, wait=True, timeout=600, extra_vars=ex_vars, limits=limit)
                except (exc.UsageError, exc.JobFailure, exc.Timeout) as err:
                    print (err)
                # print('Job failed? : {}'.format(result['failed']))

        print('Done with test!')

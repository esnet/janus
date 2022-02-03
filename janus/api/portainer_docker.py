import six
import json
import logging
from portainer_api.api_client import ApiClient
from portainer_api.rest import ApiException

log = logging.getLogger(__name__)

class PortainerDockerApi(object):
    def __init__(self, api_client=None):
        if api_client is None:
            api_client = ApiClient()
        self.api_client = api_client

    # Images
    def get_images(self, pid, **kwargs):
        kwargs['_return_http_data_only'] = True
        res = self._call("/endpoints/{}/docker/images/json".format(pid),
                         "GET", None, **kwargs)
        string = res.read().decode('utf-8')
        return json.loads(string)

    def pull_image(self, pid, img, tag):
        kwargs = dict()
        kwargs['_return_http_data_only'] = True
        kwargs['query'] = {"fromImage": img,
                           "tag": tag}
        res = self._call("/endpoints/{}/docker/images/create".format(pid),
                         "POST", None, **kwargs)
        string = res.read().decode('utf-8')
        ret = {"status": res.status}
        for s in string.splitlines():
            ret.update(json.loads(s))
        return ret

    # Containers
    def get_containers(self, pid, **kwargs):
        kwargs['_return_http_data_only'] = True
        res = self._call("/endpoints/{}/docker/containers/json".format(pid),
                         "GET", None, **kwargs)
        string = res.read().decode('utf-8')
        return json.loads(string)

    def inspect_container(self, pid, cid, **kwargs):
        kwargs['_return_http_data_only'] = True
        res = self._call("/endpoints/{}/docker/containers/{}/json".format(pid, cid),
                         "GET", None, **kwargs)
        string = res.read().decode('utf-8')
        return json.loads(string)

    def create_container(self, pid, image, name=None, **kwargs):
        body = {'Image': image}
        params = ['HostName', 'HostConfig', 'NetworkingConfig', 'ExposedPorts',
                  'Env', 'Tty', "MacAddress", 'StopSignal']
        for k, v in six.iteritems(kwargs):
            if k in params:
                body[k] = v
        kwargs = dict()
        kwargs['_return_http_data_only'] = True
        if name:
            kwargs['query'] = {"name": name}
        res = self._call("/endpoints/{}/docker/containers/create".format(pid),
                         "POST", body, **kwargs)
        if (res.status == 502):
            return {'status': '{} Bad gateway'.format(res.status)}
        string = res.read().decode('utf-8')
        return json.loads(string)

    def start_container(self, pid, cid, **kwargs):
        kwargs['_return_http_data_only'] = True
        res = self._call("/endpoints/{}/docker/containers/{}/start".format(pid, cid),
                         "POST", None, **kwargs)
        if (res.status == 200 or res.status == 204):
            return {'status': '{} OK'.format(res.status)}
        string = res.read().decode('utf-8')
        return json.loads(string)

    def stop_container(self, pid, cid, **kwargs):
        kwargs['_return_http_data_only'] = True
        status = 204
        try:
            res = self._call("/endpoints/{}/docker/containers/{}/stop".format(pid, cid),
                             "POST", None, **kwargs)
            status = res.status
        except ApiException as e:
            status = e.status
            # not modified, container is already stopped
            if e.status == 304:
                pass
            else:
                raise e
        return {'status': '{}'.format(status),
                'node_id': pid, 'container_id': cid}

    def remove_container(self, pid, cid, **kwargs):
        kwargs['_return_http_data_only'] = True
        res = self._call("/endpoints/{}/docker/containers/{}".format(pid, cid),
                         "DELETE", None, **kwargs)
        if (res.status == 204):
            return {'status': '{} OK'.format(res.status)}
        string = res.read().decode('utf-8')
        return json.loads(string)

    # Networks
    def get_networks(self, pid, **kwargs):
        kwargs['_return_http_data_only'] = True
        res = self._call("/endpoints/{}/docker/networks".format(pid),
                         "GET", None, **kwargs)
        string = res.read().decode('utf-8')
        return json.loads(string)

    def connect_network(self, pid, nid, cid, **kwargs):
        body = {'Container': cid}
        params = ['EndpointConfig']
        for k, v in six.iteritems(kwargs):
            if k in params:
                body[k] = v
        kwargs = dict()
        kwargs['_return_http_data_only'] = True
        res = self._call("/endpoints/{}/docker/networks/{}/connect".format(pid, nid),
                         "POST", body, **kwargs)
        string = res.read().decode('utf-8')
        if (res.status == 200):
            return {'status': '200 OK'}
        return json.loads(string)

    # Exec
    def exec_create(self, pid, cid, **kwargs):
        body = dict()
        params = ['AttachStdin', 'AttachStdout', 'AttachStderr', 'DetachKeys', 'Cmd', 'Env', 'Tty']
        for k, v in six.iteritems(kwargs):
            if k in params:
                body[k] = v
        kwargs = dict()
        kwargs['_return_http_data_only'] = True
        res = self._call("/endpoints/{}/docker/containers/{}/exec".format(pid, cid),
                         "POST", body, **kwargs)
        string = res.read().decode('utf-8')
        return json.loads(string)

    def exec_start(self, pid, eid, **kwargs):
        kwargs['_return_http_data_only'] = True
        body = {"Detach": False,
                "Tty": True}
        res = self._call("/endpoints/{}/docker/exec/{}/start".format(pid, eid),
                         "POST", body, **kwargs)
        string = res.read().decode('utf-8')
        return {"response": string}

    def _call(self, url, method, body, **kwargs):
        all_params = ['body', 'query']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " in PortainerDockerApi" % key
                )
            params[key] = val
        del params['kwargs']

        collection_formats = {}

        path_params = {}

        query_params = []
        if 'query' in params:
            query_params = params['query']

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = body
        if 'body' in params:
            body_params = params['body']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = ['jwt']  # noqa: E501

        log.debug("Portainer-Docker call: {} {} body={}".format(method, url, body))

        return self.api_client.call_api(
            url, method,
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', False),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

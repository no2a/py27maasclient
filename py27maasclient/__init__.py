#!/usr/bin/env python

import json
import time

from oauthlib import oauth1
import requests_oauthlib


class MAASError(Exception):
    pass


def _get_json(res, ok_check=False):
    if ok_check and not res.ok:
        raise MAASError('status=%s content=%s' % (res.status_code, res.text))
    content = res.text
    headers = res.headers
    status_code = res.status_code
    ct = headers.get('content-type')
    # content-type is expected to be "application/json; charset=utf-8"
    if not ct or ct.partition(';')[0] != 'application/json':
        raise MAASError(
            'unexpected non-json response: status=%s headers=%s content=%s'
            % (status_code, headers, content))
    try:
        obj = json.loads(content)
    except ValueError:
        raise MAASError(
            'failed to parse json: status=%s headers=%s content=%s'
            % (status_code, headers, content))
    return obj


class Client(object):

    def __init__(self, url, api_key):
        self.url = url.rstrip('/')
        client_key, owner_key, owner_secret = api_key.split(':')
        self.session = requests_oauthlib.OAuth1Session(
            client_key=client_key,
            resource_owner_key=owner_key,
            resource_owner_secret=owner_secret,
            signature_method=oauth1.SIGNATURE_PLAINTEXT)

    def _url(self, path):
        return '%s/%s' % (self.url, path.lstrip('/'))

    def get(self, path, **kwargs):
        headers = kwargs.setdefault('headers', {})
        headers['accept'] = 'application/json'
        return self.session.get(self._url(path), **kwargs)

    def put(self, path, data, **kwargs):
        headers = kwargs.setdefault('headers', {})
        headers['accept'] = 'application/json'
        return self.session.put(self._url(path), data=data, **kwargs)

    def post(self, path, data, **kwargs):
        headers = kwargs.setdefault('headers', {})
        headers['accept'] = 'application/json'
        return self.session.post(self._url(path), data=data, **kwargs)

    def delete(self, path, **kwargs):
        headers = kwargs.setdefault('headers', {})
        headers['accept'] = 'application/json'
        return self.session.delete(self._url(path), **kwargs)

    def get_node_id(self, hostname):
        short_hostname, _, domain = hostname.partition('.')
        if not domain:
            res = self.get('/nodes/?hostname=%s' % short_hostname)
        else:
            res = self.get('/nodes/?hostname=%s&domain=%s'
                           % (short_hostname, domain))
        nodes = _get_json(res)
        if nodes:
            assert len(nodes) == 1
            return nodes[0]['system_id']
        return None

    def get_machine(self, hostname):
        node_id = self.get_node_id(hostname)
        if not node_id:
            return None
        return Machine(self, node_id)

    def enlist_and_commission(self, hostname, mac_addresses, power_type,
                              power_parameters):
        short_hostname, _, domain = hostname.partition('.')
        data = [('hostname', short_hostname),
                ('architecture', 'amd64'),
                ('power_type', power_type),
                ]
        if domain:
            data.append(('domain', domain))
        for i in mac_addresses:
            data.append(('mac_addresses', i))
        for k, v in power_parameters.items():
            data.append(('power_parameters_%s' % k, v))
        res = self.post('/machines/', data=data)
        return self.get_machine(hostname)


class Machine(object):

    def __init__(self, client, system_id):
        self.client = client
        self.system_id = system_id

    def get_detail(self):
        res = self.client.get('/machines/%s/' % self.system_id)
        return _get_json(res, ok_check=True)

    def commission(self):
        res = self.client.post(
            '/machines/%s/?op=commission' % self.system_id, data=None)
        return _get_json(res, ok_check=True)

    def allocate(self):
        data = {'system_id': self.system_id}
        res = self.client.post('/machines/?op=allocate', data=data)
        return _get_json(res, ok_check=True)

    def deploy(self):
        res = self.client.post(
            '/machines/%s/?op=deploy' % self.system_id, data=None)
        return _get_json(res, ok_check=True)

    def release(self):
        res = self.client.post(
            '/machines/%s/?op=release' % self.system_id, data=None)
        return _get_json(res, ok_check=True)

    def delete(self):
        res = self.client.delete('/machines/%s/' % self.system_id)
        return _get_json(res, ok_check=True)

    def poll(self, return_on, continue_on, timeout):
        wait_total = 0
        backoff = 3
        while True:
            t = time.time()
            info = self.get_detail()
            status = info['status_name']
            if status in return_on:
                return
            elif status in continue_on:
                if wait_total > timeout:
                    break
                # Can timeout even if the time goes back
                dt = max(time.time() - t, 0)
                time.sleep(backoff)
                wait_total += dt + backoff
                backoff = min(10, backoff * 2)
                continue
            else:
                raise MAASError('unexpected status: %s' % status)
        msg = ('the machine did not reach the expected status %s within '
               'the time limit %ds (the last status is %s)'
               % (return_on, timeout, status))
        raise MAASError(msg)

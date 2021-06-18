from collections import defaultdict
import random

import httpx


USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]

RATE_LIMIT_WAIT_TIMES = {
    'get:/1.1/followers/list.json': 9 * 60,  # 9 minutes
    'get:/1.1/friends/list.json': 9 * 60,
    'post:/1.1/users/lookup.json': 9 * 60,
    'get:/1.1/friends/ids.json': 9 * 60
}


class TrioHttpSession(object):

    def __init__(self, ssl=True, verify=True, proxy_url=None, auth=None, limits=None):

        if proxy_url and 'crawlera.com' in proxy_url:
            assert ssl is False

        base_headers = None
        if proxy_url:
            base_headers = {'User-Agent': random.choice(USER_AGENTS)}

        if limits is None:
            limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)

        self.client = httpx.AsyncClient(
            headers=base_headers, verify=ssl, auth=auth,
            proxies=proxy_url, timeout=40
        )
        self.request_lock = None
        self.proxy_url = proxy_url

        self.limit_remaining_by_url_prefix = defaultdict(list)
        self.response_statuses = defaultdict(list)

    async def get(self, url, params=None, timeout=None, headers=None, auth=None):
        if self.request_lock:
            await self.request_lock.acquire()

        resp = await self.client.get(
            url, params=params, timeout=timeout, headers=headers, auth=auth
        )

        if self.request_lock:
            self.request_lock.release()
        '''
        if resp.status_code == 429:
            print('warning: rate limit hit (429)')
            await trio.sleep(600)
            resp = await self.client.get(
                url, params=params, timeout=timeout, headers=headers
            )
        '''
        #await self.process_response(resp)
        return resp.status_code, resp.text, resp

    async def post(self, url, data=None, json=None, headers=None, auth=None):
        kwargs = {'headers': headers, 'auth': auth}
        if json:
            kwargs['json'] = json
        if data:
            kwargs['data'] = data

        if self.request_lock:
            await self.request_lock.acquire()

        resp = await self.client.post(url, **kwargs)

        if self.request_lock:
            self.request_lock.release()
        '''
        if resp.status_code == 429:
            await trio.sleep(600)
            resp = await self.client.post(
                url, data=data, json=json, headers=headers
            )
        '''
        # await self.process_response(resp)
        return resp.status_code, resp.text, resp

    async def close(self):
        await self.client.aclose()


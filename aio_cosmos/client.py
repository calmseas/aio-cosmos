import asyncio

from . import auth, http_constants
from aio_cosmos import __version__, __cosmos_api_version__

from datetime import datetime

import aiohttp
from aiohttp.client_reqrep import ClientResponse

from typing import Optional, Union, Any, AsyncGenerator, Dict, List, Tuple
import random

from contextlib import asynccontextmanager


class CosmosError(Exception):

    def __init__(self, http_status_code: int, response: dict, message: str):
        self.http_status_code = http_status_code
        self.response = response
        if 'message' in self.response:
            self.message = message + f"\n --> response: {self.response['message']}"
        else:
            self.message = message + f"\n --> response: {self.response}"
        super().__init__(self.message)

    def __repr__(self) -> str:
        return f'CosmosError HTTP{self.http_status_code}: {self.response["Message"]}'


DEFAULT_HEADERS = {
    http_constants.HttpHeaders.UserAgent: f'aio-cosmos/python-cosmos-async-sdk/{__version__}',
    http_constants.HttpHeaders.Version: __cosmos_api_version__,
    http_constants.HttpHeaders.CacheControl: 'no-cache',
    http_constants.HttpHeaders.IsContinuationExpected: 'False',
    http_constants.HttpHeaders.ConsistencyLevel: 'Session',
    http_constants.HttpHeaders.SessionToken: '',
    http_constants.HttpHeaders.Accept: 'application/json'
}


def is_master_resource(resourceType):
    return resourceType in (
        http_constants.ResourceType.Offer,
        http_constants.ResourceType.Database,
        http_constants.ResourceType.User,
        http_constants.ResourceType.Permission,
        http_constants.ResourceType.Topology,
        http_constants.ResourceType.DatabaseAccount,
        http_constants.ResourceType.PartitionKeyRange,
        http_constants.ResourceType.Collection,
    )


async def on_request_end(session, trace_config_ctx, params):
    print("Ending %s request for %s. I sent: %s" % (params.method, params.url, params.headers))
    print('Sent headers: %s' % params.response.request_info.headers)


class CosmosClient:

    def __init__(self, endpoint: str, master_key: str, debug: bool = False, raise_on_failure: bool = True):
        self.endpoint = endpoint if endpoint.endswith('/') else endpoint + '/'
        self.writable_endpoints = [{'databaseAccountEndpoint': self.endpoint}]
        self.readable_endpoints = [{'databaseAccountEndpoint': self.endpoint}]
        self.server_details = None
        self.master_key = master_key
        self.session_token = None
        self.raise_on_failure = raise_on_failure
        if debug:
            trace_config = aiohttp.TraceConfig()
            trace_config.on_request_end.append(on_request_end)
            connector = aiohttp.TCPConnector(ssl=False)
            self.session = aiohttp.ClientSession(raise_for_status=False, connector=connector, trace_configs=[trace_config])
        else:
            self.session = aiohttp.ClientSession(raise_for_status=False)

    async def connect(self):
        headers = self._get_headers(http_constants.HttpMethods.Get, None, "")

        async with self.session.get(self.endpoint, headers=headers) as response:
            self.server_details = await response.json()
            self.writable_endpoints = self.server_details['writableLocations']
            self.readable_endpoints = self.server_details['readableLocations']

    async def close(self):
        await self.session.close()

    def _get_readable(self):
        return random.choice(self.readable_endpoints)['databaseAccountEndpoint']

    def _get_writable(self):
        return random.choice(self.readable_endpoints)['databaseAccountEndpoint']

    def _get_headers(self,
                     method: str,
                     resource_id: Optional[str],
                     resource_type: str,
                     is_query=False,
                     throughput: Optional[int] = None,
                     autoscale_ceiling: Optional[int] = None,
                     upsert: Optional[bool] = None,
                     indexed: Optional[bool] = None,
                     session_token: Optional[str] = None):
        headers = DEFAULT_HEADERS.copy()

        if session_token is not None and not is_master_resource(resource_type):
            headers[http_constants.HttpHeaders.SessionToken] = session_token

        if method in (http_constants.HttpMethods.Put, http_constants.HttpMethods.Post) and is_query:
            headers[http_constants.HttpHeaders.ContentType] = 'application/query+json'
            headers[http_constants.HttpHeaders.IsQuery] = str(is_query)
            del headers[http_constants.HttpHeaders.IsContinuationExpected]

        if upsert:
            headers[http_constants.HttpHeaders.IsUpsert] = str(upsert)

        if indexed is not None:
            headers[http_constants.HttpHeaders.IndexingDirective] = "Include" if indexed else "Exclude"

        if throughput is not None and autoscale_ceiling is not None:
            raise CosmosError('Only one of throughput or autoscale_ceiling can be specified')

        if throughput is not None:
            headers[http_constants.HttpHeaders.OfferThroughput] = str(throughput)

        if autoscale_ceiling is not None:
            headers[http_constants.HttpHeaders.OfferAutopilot] = str(autoscale_ceiling)

        headers[http_constants.HttpHeaders.XDate] = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

        headers[http_constants.HttpHeaders.Authorization] = auth.get_authorization_header(
            method, resource_id, resource_type, headers, self.master_key)

        return headers

    async def _handle_response(self,
                               response: ClientResponse,
                               error_message: str,
                               manage_session: bool = False,
                               subkey: Optional[str] = None):
        data = await response.json()

        if response.status >= 400 and self.raise_on_failure:
            if self.raise_on_failure:
                raise CosmosError(response.status, data, error_message)

        session_token = None
        if manage_session:
            self.session_token = response.headers.get(http_constants.HttpHeaders.SessionToken)
            session_token = self.session_token

        return {
            'status': 'failed' if response.status >= 400 else 'ok',
            'code': response.status,
            'session_token': session_token,
            'error': error_message if response.status >= 400 else None,
            'data': data[subkey] if subkey is not None else data
        }

    async def list_databases(self):
        headers = self._get_headers(http_constants.HttpMethods.Get, None, "dbs")

        async with self.session.get(f'{self._get_writable()}/dbs', headers=headers) as response:
            return await self._handle_response(response, 'Could not list databases')

    async def create_database(self, name: str,
                              throughput: Optional[int] = None,
                              autoscale_ceiling: Optional[int] = None) -> Dict[str, Any]:
        headers = self._get_headers(http_constants.HttpMethods.Post, None, "dbs",
                                    throughput=throughput, autoscale_ceiling=autoscale_ceiling)

        async with self.session.post(f'{self._get_writable()}/dbs', headers=headers, json={"id": name}) as response:
            return await self._handle_response(response, f"Could not create database: {name}")

    async def delete_database(self, name: str) -> Dict[str, Any]:
        headers = self._get_headers(http_constants.HttpMethods.Delete, f"dbs/{name}", "dbs")

        async with self.session.delete(f'{self._get_writable()}/dbs/{name}', headers=headers, ) as response:
            return await self._handle_response(response, f"Could not delete database: {name}")

    async def create_container(self, database: str,
                               container: str,
                               partition_key: str,
                               throughput: Optional[int] = None,
                               autoscale_ceiling: Optional[int] = None) -> Dict[str, Any]:
        headers = self._get_headers(http_constants.HttpMethods.Post, f'dbs/{database}', 'colls',
                                    throughput=throughput, autoscale_ceiling=autoscale_ceiling)

        json = {
            'id': container,
            'partitionKey': {
                'paths': [
                    f'{partition_key}'
                ],
                'kind': 'Hash',
                'Version': 2
            }
        }

        async with self.session.post(f'{self._get_writable()}/dbs/{database}/colls/', headers=headers,
                                     json=json) as response:
            return await self._handle_response(response, f"Could not create container: {database}:{container}")

    async def delete_container(self, database: str, container: str) -> Dict[str, Any]:
        headers = self._get_headers(http_constants.HttpMethods.Delete, f'dbs/{database}/colls/{container}', 'colls')

        async with self.session.delete(f'{self._get_writable()}/dbs/{database}/colls/{container}/',
                                       headers=headers) as response:
            return await self._handle_response(response, f"Could not delete container: {database}:{container}")

    async def create_document(self, database: str,
                              container: str,
                              json: dict,
                              partition_key: Any,
                              upsert: Optional[bool] = None,
                              indexed: Optional[bool] = None,
                              session_token: Optional[str] = None) -> Dict[str, Any]:
        headers = self._get_headers(http_constants.HttpMethods.Post, f'dbs/{database}/colls/{container}', 'docs',
                                    upsert=upsert, indexed=indexed,
                                    session_token=session_token if session_token is not None else self.session_token)
        headers[http_constants.HttpHeaders.PartitionKey] = f'["{partition_key}"]'

        async with self.session.post(f'{self._get_writable()}/dbs/{database}/colls/{container}/docs',
                                     headers=headers, json=json) as response:
            return await self._handle_response(response, f"Could not create document in {database}:{container}",
                                               manage_session=True)

    async def create_documents(self, database: str,
                               container: str,
                               json: List[Tuple[Dict[str, Any], Any]],
                               upsert: Optional[bool] = None,
                               indexed: Optional[bool] = None,
                               session_token: Optional[str] = None) -> List[Dict[str, Any]]:
        headers = self._get_headers(http_constants.HttpMethods.Post, f'dbs/{database}/colls/{container}', 'docs',
                                    upsert=upsert, indexed=indexed,
                                    session_token=session_token if session_token is not None else self.session_token)

        # See if we can avoid creating new headers and auth sig for each request or managing the session
        # TODO: need to test this works for large datasets as the http date may get out of sync
        async def write_document(jsondoc: Tuple[Dict[str, Any], Any], header_copy: dict) -> Dict[str, Any]:
            body, partition_key = jsondoc
            headers[http_constants.HttpHeaders.PartitionKey] = f'["{partition_key}"]'
            async with self.session.post(f'{self._get_writable()}/dbs/{database}/colls/{container}/docs',
                                         headers=headers, json=body) as response:
                return await self._handle_response(response, f"Could not create document in {database}:{container}",
                                                   manage_session=False)

        return await asyncio.gather(*[write_document(x, headers.copy()) for x in json])

    async def delete_document(self, database: str, container: str, doc_id: str, partition_key: Any) -> Dict[str, Any]:
        headers = self._get_headers(http_constants.HttpMethods.Delete,
                                    f'dbs/{database}/colls/{container}/docs/{doc_id}', 'docs')
        headers[http_constants.HttpHeaders.PartitionKey] = f'["{partition_key}"]'

        async with self.session.delete(f'{self._get_writable()}/dbs/{database}/colls/{container}/docs/{doc_id}',
                                       headers=headers) as response:
            return await self._handle_response(response, f"Could not delete document: {database}:{container}:{doc_id}",
                                               manage_session=True)

    async def get_document(self, database: str, container: str, doc_id: str, partition_key: Any) -> Dict[str, Any]:
        headers = self._get_headers(http_constants.HttpMethods.Get,
                                    f'dbs/{database}/colls/{container}/docs/{doc_id}', 'docs')
        headers[http_constants.HttpHeaders.PartitionKey] = f'["{partition_key}"]'

        async with self.session.get(f'{self._get_writable()}/dbs/{database}/colls/{container}/docs/{doc_id}',
                                    headers=headers) as response:
            return await self._handle_response(response, f"Could not get document: {database}:{container}:{doc_id}",
                                               manage_session=True)

    async def query_documents(self,
                              database: str,
                              container: str,
                              query: str,
                              partition_key: Optional[Any] = None,
                              enable_cross_partition_query: Optional[bool] = False,
                              session_token: Optional[str] = None) -> AsyncGenerator:

        continuation = None
        session_token = session_token if session_token is None else self.session_token
        while True:
            headers = self._get_headers(http_constants.HttpMethods.Post, f'dbs/{database}/colls/{container}',
                                        'docs', is_query=True, session_token=session_token)
            if enable_cross_partition_query:
                headers[http_constants.HttpHeaders.EnableCrossPartitionQuery] = 'True'
            else:
                headers[http_constants.HttpHeaders.PartitionKey] = f'["{partition_key}"]'

            if continuation is not None:
                headers[http_constants.HttpHeaders.Continuation] = continuation

            json = {
                'query': query,
                'parameters': []
            }

            async with self.session.post(f'{self._get_writable()}/dbs/{database}/colls/{container}/docs',
                                         headers=headers, json=json) as response:
                res = await self._handle_response(response, f"Could not query documents",
                                                  manage_session=True, subkey='Documents')

                yield res

                continuation = response.headers.get(http_constants.HttpHeaders.Continuation)

                session_token = response.headers.get(http_constants.HttpHeaders.SessionToken)

                if continuation is None:
                    return



@asynccontextmanager
async def get_client(endpoint: str, key: str, debug: bool = False, raise_on_failure: bool = False) -> CosmosClient:
    client = CosmosClient(endpoint, key, debug, raise_on_failure)
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


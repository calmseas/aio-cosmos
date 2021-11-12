from aio_cosmos import __version__
from aio_cosmos.client import CosmosClient, get_client
import os
import pytest

from azure.cosmos import PartitionKey, ContainerProxy, CosmosClient as cc

from uuid import uuid4

def test_version():
    assert __version__ == '0.2.2'

@pytest.mark.asyncio
async def i_test_list_db():
    key = os.getenv('MASTER_KEY')
    endpoint = os.getenv('ENDPOINT')

    async with get_client(endpoint, key) as client:
        databases = await client.list_databases()
        print(databases)

@pytest.mark.asyncio
async def test_create_db():
    key = os.getenv('MASTER_KEY')
    endpoint = os.getenv('ENDPOINT')

    async with get_client(endpoint, key, raise_on_failure=False) as client:
        number = 82
        print(await client.create_database(f'test-db-async-{number}'))
        await client.create_container(f'test-db-async-{number}', 'test-container-async', '/account')
        doc_id = str(uuid4())
        res = await client.create_document(f'test-db-async-{number}', 'test-container-async',
                                           {'id': doc_id, 'account': 'Account-1', 'description': 'tax surcharge'}, partition_key="Account-1")
        print(res)

        docs = [
            ({'id': str(uuid4()), 'account': 'Account-1', 'description': 'invoice paid'}, 'Account-1'),
            ({'id': str(uuid4()), 'account': 'Account-1', 'description': 'VAT remitted'}, 'Account-1'),
            ({'id': str(uuid4()), 'account': 'Account-1', 'description': 'interest paid'}, 'Account-1'),
            ({'id': str(uuid4()), 'account': 'Account-2', 'description': 'annual fees'}, 'Account-2'),
            ({'id': str(uuid4()), 'account': 'Account-2', 'description': 'commission'}, 'Account-2'),

        ]

        import time
        start = time.time()
        res = await client.create_documents(f'test-db-async-{number}', 'test-container-async', docs)
        print(f'time to save five docs: {time.time() - start}s')
        print(res)

        res = await client.get_document(f'test-db-async-{number}', 'test-container-async', doc_id=doc_id, partition_key="Account-1")
        print(f'document from get: {res["data"]}')
        async for result in client.query_documents(f'test-db-async-{number}', 'test-container-async',
                                                query="select * from r where r.account = 'Account-1'",
                                                partition_key="Account-1"):
            print(f'doc returned by query: {result["data"]}')
        await client.delete_document(f'test-db-async-{number}', 'test-container-async', doc_id=doc_id, partition_key="Account-1")
        await client.delete_container(f'test-db-async-{number}', 'test-container-async')
        await client.delete_database(f'test-db-async-{number}')


def i_test_create_db_az():
    key = os.getenv('MASTER_KEY')
    endpoint = os.getenv('ENDPOINT')
    client = cc(endpoint, key)
    number = 20
    #database = client.create_database(f'test-az-sync-{number}')
    database = client.get_database_client(f'test-db-async-76')
    container: ContainerProxy = database.get_container_client('test-container-async')
    #doc_id = str(uuid4())
    #container.create_item({'id': doc_id, 'account': 'Account-1'})
    results = container.query_items(query="select * from r where r.account = 'Account-1'", partition_key='Account-1')
    print(list(results))
    #database.delete_container('test-container-sync')
    #client.delete_database(f'test-az-sync-{number}')


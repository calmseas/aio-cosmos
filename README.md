# aio-cosmos
Asyncio SDK for Azure Cosmos DB. This library is intended to be a very thin asyncio wrapper around the [Azure Comsos DB Rest API][1]. 
It is not intended to have feature parity with the Microsoft Azure SDKs but to provide async versions of the most commonly used interfaces.

[1]: (https://docs.microsoft.com/en-us/rest/api/cosmos-db/)

## Feature Support
### Databases
✅ List\
✅ Create\
✅ Delete

### Containers
✅ Create\
✅ Delete

### Documents
✅ Create Single\
✅ Create Concurrent Multiple\
✅ Delete\
✅ Get\
✅ Query

## Limitations

The library currently only supports Session level consistency, this may change in the future. 
For concurrent writes the maximum concurrency level is based on a maximum of 100 concurrent
connections from the underlying aiohttp library. This may be exposed to the as a client 
setting in a future version.

Sessions are managed automatically for document operations. The session token is returned in the
result so it is possible to manage sessions manually by providing this value in session_token to
the appropriate methods. This facilitates sending the token value back to an end client in a
session cookie so that writes and reads can maintain consistency across multiple instances of
Cosmos.

## Installation

```shell
pip install aio-cosmos
```

## Usage

### Client Setup and Basic Usage

The client can be instantiated using either the context manager as below or directly using the CosmosClient class.
If using the CosmosClient class directly the user is responsible for calling the .connect() and .close() methods to
ensure the client is boot-strapped and resources released at the appropriate times.

```python
from aio_cosmos.client import get_client

async with get_client(endpoint, key) as client:
    await client.create_database('database-name')
    await client.create_container('database-name', 'container-name', '/partition_key_document_path')
    doc_id = str(uuid4())
    res = await client.create_document(f'database-name', 'container-name',
                                       {'id': doc_id, 'partition_key_document_path': 'Account-1', 'description': 'tax surcharge'}, partition_key="Account-1")
```

### Querying Documents

Documents can be queried using the query_documents method on the client. This method returns an AsyncGenerator and should
be used in an async for statement as below. The generator automatically handles paging for large datasets. If you don't
wish to iterate through the results use a list comprehension to collate all of them.

```python
async for doc in client.query_documents(f'database-name', 'container-name',
                                        query="select * from r where r.account = 'Account-1'",
                                        partition_key="Account-1"):
    print(f'doc returned by query: {doc}')
```

### Concurrent Writes / Multiple Documents

The client provides the ability to issue concurrent document writes using asyncio/aiohttp. Each document is represented
by a tuple of (document, partition key) as below.

```python
docs = [
    ({'id': str(uuid4()), 'account': 'Account-1', 'description': 'invoice paid'}, 'Account-1'),
    ({'id': str(uuid4()), 'account': 'Account-1', 'description': 'VAT remitted'}, 'Account-1'),
    ({'id': str(uuid4()), 'account': 'Account-1', 'description': 'interest paid'}, 'Account-1'),
    ({'id': str(uuid4()), 'account': 'Account-2', 'description': 'annual fees'}, 'Account-2'),
    ({'id': str(uuid4()), 'account': 'Account-2', 'description': 'commission'}, 'Account-2'),
]

res = await client.create_documents(f'database-name', 'container-name', docs)
```

### Results

Results are returned in a dictionary of the following format:

```python
{
    'status': str,
    'code': int,
    'session_token': Optional[str],
    'error': Optional[str],
    'data': Union[dict,list]
}
```
status will be either 'ok' or 'failed'
code is the integer HTTP response code
session_token is the string session code vector returned by Cosmos
error is a string error message to provide context to a failed status
data is either the data or error return of the operation from Cosmos

Note, to see an error return in the above format you must pass ```raise_on_failure=False``` to the client instantiation.

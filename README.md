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

## Installation

```shell
pip install aio-cosmos
```


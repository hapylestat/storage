

from .generic import GenericStorage, StorageCredentials
from .mongodb import MongoStorage
from .minio import MinIOStorage


__storages = [MongoStorage, MinIOStorage]

_STORAGES = {}

for storage in __storages:
  for name in storage.name():
    _STORAGES[name] = storage


class StorageFactory(object):
  @classmethod
  def get(cls, connection_string: str):
    """
    Connection string example:
        mongodb://hell.nxa.io:27000/ambari_binaries

        storage://user:pass@host:port/database?name=value&name=value

    :param connection_string:
    :return:
    """
    storage_name, _, rest = connection_string.partition("://")
    if storage_name not in _STORAGES:
      raise ValueError("Storage {} is not supported".format(storage_name))

    chunk, _, rest = rest.partition("/")
    # chunk 0 - auth, host, port

    if "@" in chunk:
      auth, _, chunk = chunk.partition("@")
      auth = StorageCredentials.from_string(auth)
    else:
      auth = None

    host, _, port = chunk.partition(":")
    bucket, _, options = rest.partition("?")
    options = {k: v for k, _, v in [entry.partition("=") for entry in options.split("&") if entry]}

    _cls = _STORAGES[storage_name]
    assert issubclass(_cls, GenericStorage)

    return _cls(host, port, bucket, auth, options)

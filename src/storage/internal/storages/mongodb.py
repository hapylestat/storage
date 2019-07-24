from enum import Enum
from typing import Dict, Iterable, List

from .generic import GenericStorage, StorageCredentials, FileItemRecord, SizeScale, BucketItemRecord, FileIn, \
  FileOut

import gridfs
from bson import ObjectId
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase
from pymongo.errors import ConfigurationError


class GridFSTable(Enum):
  files = "files"
  chunks = "chunks"


class MongoStorage(GenericStorage):
  _client: MongoClient = None
  _db: MongoDatabase = None

  def __init__(self, host: str, port: str, bucket: str, auth: StorageCredentials, options: Dict[str, str]):
    super().__init__(host, port, bucket, auth, options)

  @classmethod
  def name(cls) -> List[str]:
    return ["mongodb"]

  def connect(self):
    url = "{type}://{auth}{host}:{port}/{database}{options}".format(
      type=self.name()[0],
      auth="{}:{}@".format(self._auth.user, self._auth.password) if self._auth else "",
      host=self._host,
      port=self._port,
      database=self._database,
      options="?{}".format("&".join(["{}={}".format(k, v) for k, v in self._options.items()]))
    )
    self._client = MongoClient(url)
    try:
      self._db = self._client.get_database()
    except ConfigurationError as e:
      raise ValueError("Please check configuration url: {}".format(str(e)))

  def _fs(self, bucket: str) -> gridfs.GridFS:
    return gridfs.GridFS(self._db, collection=bucket) if self._database else None

  def disconnect(self):
    self._client.close()

  def bucket_list(self) -> Iterable[BucketItemRecord]:
    done_list = {}
    # consists from array records [ int, dict ]. Where int is an amount of processed tables and second - value

    for collection in self._db.list_collections():
      raw_name: str = collection["name"]
      name: str = raw_name[: raw_name.rfind(".")]
      col_stats: Dict = self._db.command("collStats", raw_name, scale=SizeScale.bytes)

      try:
        record_type: GridFSTable = GridFSTable(raw_name.rpartition(".")[2])
      except ValueError:
        record_type: None = None

      if name not in done_list:
        done_list[name] = [0, BucketItemRecord(
          bid=[],
          size=SizeScale(0),
          name=name,
          raw=[]
        )]

      tables_done: int = done_list[name][0]
      bucket: BucketItemRecord = done_list[name][1]

      if record_type == GridFSTable.chunks:
        bucket.size += col_stats["size"]
        tables_done += 1
      elif record_type == GridFSTable.files:
        bucket.files = col_stats["count"]
        if bucket.files > 0:
          try:
            collection_upload_time = list(self._db.get_collection(raw_name).find(limit=1))[0]["uploadDate"]
            """:type collection_upload_time datetime"""
            bucket.date = "{0:%Y-%m-%d %H:%M:%S}".format(collection_upload_time)

          except IndexError:
            pass
        tables_done += 1

      done_list[name][0] = tables_done
      bucket.raw.append(raw_name)

      if tables_done == 2:
        yield bucket
        done_list[name] = None

  def bucket_exists(self, name: str) -> bool:
    return "{}.{}".format(name, GridFSTable.chunks.value) in self._db.list_collection_names()

  def drop_bucket(self, name: str):
    drop_list = [
      "{}.{}".format(name, GridFSTable.chunks.value),
      "{}.{}".format(name, GridFSTable.files.value)
    ]

    for c in drop_list:
      self._db.drop_collection(c)

  def list(self, bucket: str, filename: str or None = None) -> Iterable[FileItemRecord]:
    fs = self._fs(bucket)

    files = fs.find() if not filename else fs.find({'filename': {'$regex': filename}})

    for f in files:
      assert isinstance(f, gridfs.GridOut)
      f_out = FileOut(f._id, f.chunk_size, f.length, "md5", f.md5, f)

      yield FileItemRecord(f._id, f.filename, SizeScale(f_out.size), f.upload_date, f.md5, f_out)

  def new_file(self, bucket: str, filename: str, size: int) -> FileIn:
    fs = self._fs(bucket)
    f = fs.new_file(filename=filename)
    return FileIn(f.chunk_size, lambda: f._id, lambda: f.md5, f)

  def delete(self, bucket: str, f: FileItemRecord or str):
    fs = self._fs(bucket)

    if isinstance(f, FileItemRecord):
      _id = f.fid
      if not isinstance(_id, ObjectId):
        _id = ObjectId(_id)
      fs.delete(_id)
    else:
      if not isinstance(f, ObjectId):
        f = ObjectId(f)

      fs.delete(ObjectId(f))

  def stat(self):
    pass

  def server_stats(self, scale_factor: float) -> Dict[str, str]:
    db_stats = self._db.command("dbStats", 1, scale=scale_factor)

    ssize = SizeScale(db_stats["storageSize"] if db_stats and "storageSize" in db_stats else -1)
    dsize = SizeScale(db_stats["dataSize"] if db_stats and "dataSize" in db_stats else -1)

    return {
      "server version": self._client.server_info()["version"],
      "db": db_stats["db"] if db_stats and "db" in db_stats else "<unknown>",
      "storage size": f"{ssize.size:.2f} {ssize.scale_name}",
      "data size": f"{dsize.size:.2f} {dsize.scale_name}",
      "collections": db_stats["collections"] if db_stats and "collections" in db_stats else -1
    }

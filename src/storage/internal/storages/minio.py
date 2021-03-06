import hashlib
import json
from io import StringIO, BytesIO
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Union

from minio.definitions import UploadPart
from urllib3 import HTTPResponse

from . import GenericStorage
from .generic import SizeScale, FileItemRecord, FileIn, BucketItemRecord, StorageCredentials, FileOut

from minio import Minio, helpers, signer


class WebRPCActions(object):
  login = "Web.Login"
  server_info = "Web.ServerInfo"
  storage_info = "Web.StorageInfo"


class MinIOWebRPC(object):
  _webrpc_url = None
  _webrpc_headers = {}

  def __init__(self, endpoint: str, client: Minio, auth: StorageCredentials):
    self._auth = auth
    self._client = client
    self._webrpc_url = endpoint

  def __webrpc_make_request(self, payload: Dict, method: str) -> Dict:
    return {
      "id": 1,
      "jsonrpc": "2.0",
      "params": payload,
      "method": method
    }

  def request(self, action: str, data: Dict) -> Dict or None:
    hdrs = {
      "X-Amz-Date": datetime.now().strftime("%Y%m%dT%H%M%SZ"),
      "Content-Type": "application/json"
    }
    hdrs.update(self._webrpc_headers)

    d = self.__webrpc_make_request(data, action)
    post_stream = StringIO(initial_value=json.dumps(d))

    r = self._client._http.urlopen("POST", self._webrpc_url, redirect=False, headers=hdrs, retries=5, body=post_stream)
    if r.status == 200:
      j = json.loads(r.data)
      if "result" not in j:
        return None
      return j["result"]

    return None

  def login(self) -> bool:
    r = self.request(WebRPCActions.login, {
      "username": self._auth.user,
      "password": self._auth.password
    })

    if not r or "token" not in r:
      return False

    token = r["token"]
    self._webrpc_headers.update({
      'Authorization': "Bearer {}".format(token)
    })
    return True

  def server_info(self) -> Dict:
    r = self.request(WebRPCActions.server_info, {})

    if not r:
      return {
        "version": "<unknown>",
        "memory": "<unknown>",
        "platform": "<unknown>",
        "runtime": "<unknown>"
      }
    return {
      "version": r["MinioVersion"],
      "memory": r["MinioMemory"],
      "platform": r["MinioPlatform"],
      "runtime": r["MinioRuntime"]
    }

  def storage_info(self) -> Dict:
    r = self.request(WebRPCActions.storage_info, {})
    if not r:
      return {
        "used": -1,
        "total": -1,
        "available": -1,
        "online disks": -1,
        "offline disks": -1
      }

    d = {
      "used": r["storageInfo"]["Used"],
      "total": r["storageInfo"]["Total"],
      "available": r["storageInfo"]["Available"],
      "online disks": r["storageInfo"]["Backend"]["OnlineDisks"],
      "offline disks": r["storageInfo"]["Backend"]["OfflineDisks"]
    }

    i = 0
    c = 0
    for _set in r["storageInfo"]["Backend"]["Sets"]:
      for s in _set:
        label = f"set {i} disk {c}"
        d[label] = f"{s['endpoint']}; state: {s['state']}"
        c += 1

    i += 1
    c = 0

    return d


class MinioStreamOutProxy(FileOut):
  __f_handle: HTTPResponse = None
  __client: Minio = None

  def __init__(self, client: Minio, bucket_name: str, filename: str, chunk_size: int, size: int, hash_value: str):
    def ex():
      raise NotImplementedError()

    self.__client = client
    self.filename = filename
    self.bucket_name = bucket_name

    super().__init__(hash_value, chunk_size, size, "sha256", hash_value, self)

  def __build_proxy(self):
    self.__f_handle = self.__client.get_object(self.bucket_name, self.filename)

    # hack, which allow to not use extra stat call to get the true hash more early
    if "X-Amz-Meta-Sha256" in self.__f_handle.headers:
      self.hash_value = self.__f_handle.headers.get("X-Amz-Meta-Sha256")

    self.close = self.__f_handle.close
    self.read = self.__f_handle.read
    self.readline = self.__f_handle.readline
    self.tell = self.__f_handle.tell
    self.seek = self.__f_handle.seek
    self.__iter__ = self.__f_handle.__iter__
    self.__enter__ = self.__f_handle.__enter__
    self.__exit__ = self.__f_handle.__exit__

  def read(self, size: Optional[int] = ...) -> bytes:
    if not self.__f_handle:
      self.__build_proxy()

    return self.read(size)

  def readline(self, size: int = ...) -> bytes:
    if not self.__f_handle:
      self.__build_proxy()

    return self.readline(size)

  def close(self) -> None:
    if not self.__f_handle:
      self.__build_proxy()

    return self.close()

  def tell(self) -> int:
    if not self.__f_handle:
      self.__build_proxy()

    return self.tell()

  def seek(self, offset: int, whence: int = ...) -> int:
    if not self.__f_handle:
      self.__build_proxy()

    return self.seek(offset, whence)

  def __iter__(self):
    if not self.__f_handle:
      self.__build_proxy()

    return self.__iter__()

  def __enter__(self):
    if not self.__f_handle:
      self.__build_proxy()

    return self.__enter__()


class MinIOStreamInProxy(FileIn):
  """
  Class handles 2 scenarios:
   - simple upload up to MIN_PART_SIZE
   - multipart upload
  """

  def __init__(self, client: Minio, bucket_name: str, filename: str, chunk_size: int, size: int):
    # basic checks
    helpers.is_valid_bucket_name(bucket_name)
    helpers.is_non_empty_string(bucket_name)

    self._etag = None
    self.__id = None
    super(MinIOStreamInProxy, self).__init__(chunk_size, fid=lambda: self.__id, hash_value=lambda: None)
    self._client = client
    self._bucket_name = bucket_name
    self._filename = filename
    self._headers = {
      "Content-Type": "application/octet-stream",
      "x-amz-metadata-directive": "REPLACE"
    }

    self._headers = helpers.amzprefix_user_metadata(self._headers)
    try:
      if size > helpers.MIN_PART_SIZE:
        self._uploadid = self._client._new_multipart_upload(self._bucket_name, self._filename, self._headers)
        self._part_number = 1
        self._total_parts_count, self._part_size, self._last_part_size = helpers.optimal_part_info(size, chunk_size)
        self._min_stream = None
      else:
        self._uploadid = ''
        self._part_number = 0
        self._min_stream = BytesIO()
        self._total_parts_count, self._part_size, self._last_part_size = 0, 0, 0
    except Exception as e:
      raise IOError(e)

    self._uploaded_parts = {}

  def _do_put_object(self, part_data):
    headers = {
      'Content-Length': len(part_data),
    }

    if self._client._is_ssl:
      headers['Content-Md5'] = helpers.get_md5_base64digest(part_data)
      sha256_hex = signer._UNSIGNED_PAYLOAD
    else:
      sha256_hex = helpers.get_sha256_hexdigest(part_data)

    query = {}
    if self._part_number > 0 and self._uploadid:
      query = {
        'uploadId': self._uploadid,
        'partNumber': str(self._part_number),
      }

    response = self._client._url_open(
      'PUT',
      bucket_name=self._bucket_name,
      object_name=self._filename,
      query=query,
      headers=headers,
      body=BytesIO(part_data),
      content_sha256=sha256_hex,
      preload_content=False
    )

    return response.headers['etag'].replace('"', '')

  def close(self) -> None:
    if self._min_stream:
      val = self._min_stream.getvalue()
      self._do_put_object(val)
      return

    try:
      self._client._complete_multipart_upload(self._bucket_name, self._filename, self._uploadid, self._uploaded_parts)
    except Exception as e:
      self._client._remove_incomplete_upload(self._bucket_name, self._filename, self._uploadid)
      raise
    self._uploadid = None
    self._part_number = 1
    self._uploaded_parts = {}
    self.__id = self._filename

  def write(self, b: Union[bytes, bytearray]) -> int:
    if self._min_stream:
      self._min_stream.write(b)
      return len(b)

    try:
      etag = self._do_put_object(b)

    except Exception as e:
      raise IOError(e)

    total_read = len(b)
    self._uploaded_parts[self._part_number] = UploadPart(
      self._bucket_name, self._filename, self._uploadid, self._part_number, etag, None, total_read
    )
    self._part_number += 1
    return total_read

  def writelines(self, lines: Iterable) -> None:
    b = BytesIO()
    b.writelines(lines)
    self.write(b.getvalue())

  def __enter__(self):
    self._headers = {
      "Content-Type": "application/octet-stream",
      "x-amz-metadata-directive": "REPLACE"
    }

    self._headers = helpers.amzprefix_user_metadata(self._headers)
    self._uploadid = None
    self._part_number = 1
    self._uploaded_parts = {}
    self._total_uploaded = 0
    self.__hash = hashlib.md5()

  def __exit__(self, *args, **kwargs):
    self.close()


class MinIOStorage(GenericStorage):
  _client: Minio = None
  _webrpc: MinIOWebRPC = None

  @classmethod
  def name(cls) -> List[str]:
    return ["minio", "s3"]

  def connect(self):
    is_secure = "secure" in self._options and self._options["secure"] == "true"
    endpoint = "{}:{}".format(self._host, self._port)
    self._client = Minio(
      endpoint,
      access_key=self._auth.user,
      secret_key=self._auth.password,
      secure=is_secure
    )

    self._webrpc = MinIOWebRPC(
      f"{'https://' if is_secure else 'http://'}{endpoint}/minio/webrpc",
      self._client,
      self._auth
    )

    if not self._webrpc.login():
      raise ConnectionError("Couldn't connect or auth to webrpc service")

  def disconnect(self):
    pass

  def bucket_list(self) -> Iterable[BucketItemRecord]:
    for b in self._client.list_buckets():
      objs = self._client.list_objects_v2(bucket_name=b.name, recursive=True)
      size = 0
      files = 0
      for s in objs:
        files += 1
        size += s.size

      yield BucketItemRecord(name=b.name, date=b.creation_date, files=files, size=SizeScale(size))

  def bucket_exists(self, name: str) -> bool:
    return self._client.bucket_exists(name)

  def drop_bucket(self, name: str):
    return self._client.remove_bucket(name)

  def list(self, bucket: str, filename: str or None = None) -> Iterable[FileItemRecord]:
    for fobj in self._client.list_objects_v2(bucket_name=bucket, prefix=filename, recursive=True):
      if fobj.is_dir:  # not supporting paths and directories for now
        continue
      hash_value = fobj.etag.partition("-")[0]
      f = MinioStreamOutProxy(self._client, bucket, fobj.object_name, 1024 * 1024, fobj.size, hash_value)
      yield FileItemRecord(fobj.object_name, fobj.object_name, SizeScale(fobj.size), fobj.last_modified, hash_value, f)

  def new_file(self, bucket: str, filename: str, size: int) -> FileIn:
    return MinIOStreamInProxy(self._client, bucket, filename, helpers.MIN_PART_SIZE, size)

  def delete(self, bucket: str, f: FileItemRecord or object):
    if isinstance(f, FileItemRecord):
      self._client.remove_object(bucket, f.fid)
    else:
      self._client.remove_object(bucket, f)

  def stat(self):
    pass

  def server_stats(self, scale_factor: SizeScale) -> Dict[str, str]:
    r = {}
    r.update(self._webrpc.server_info())
    r.update(self._webrpc.storage_info())

    used = SizeScale(float(r["used"]))
    total = SizeScale(float(r["total"]))
    avail = SizeScale(float(r["available"]))

    r["used"] = f"{used.size:.2f} {used.scale_name}"
    r["total"] = f"{total.size:.2f} {total.scale_name}"
    r["available"] = f"{avail.size:.2f} {avail.scale_name}"

    return r

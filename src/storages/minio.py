import json
from io import StringIO
from datetime import datetime
from typing import Dict, Iterable, List

from storages import GenericStorage
from storages.generic import SizeScale, FileItemRecord, FileIn, BucketItemRecord, StorageCredentials

from minio import Minio


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
    pass

  def new_file(self, bucket: str, filename: str) -> FileIn:
    pass

  def delete(self, bucket: str, f: FileItemRecord or object):
    pass

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

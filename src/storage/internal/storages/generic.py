from types import LambdaType
from typing import Dict, Tuple, Iterable, List

from datetime import datetime
from io import BytesIO


class SizeScale(object):
  bytes = 1.0
  kb = bytes * 1024
  mb = kb * 1024
  gb = mb * 1024
  tb = gb * 1024
  pb = tb * 1024

  def __init__(self, item: float):
    self.__size, self.__size_name = self.scale(item)
    self.__original = item

  @property
  def size(self):
    return self.__size

  @property
  def scale_name(self):
    return self.__size_name

  def __add__(self, other):
    if isinstance(other, SizeScale):
      return SizeScale(self.__original + other.__original)
    elif isinstance(other, int):
      return SizeScale(self.__original + other)
    elif isinstance(other, str) and other.isnumeric():
      return SizeScale(self.__original + int(other))

  def __sub__(self, other):
    if isinstance(other, SizeScale):
      return SizeScale(self.__original - other.__original)
    elif isinstance(other, int):
      return SizeScale(self.__original - other)
    elif isinstance(other, str) and other.isnumeric():
      return SizeScale(self.__original - int(other))

  @classmethod
  def scale(cls, size: int or float) -> Tuple[float, str]:
    """
    Scales file size according to passed scale rate and returns scaled number and scale name
    """
    scale_factor = SizeScale.kb
    _size = size
    size_type = " b"
    if size/SizeScale.kb <= scale_factor:
      _size /= SizeScale.kb
      size_type = "kb"
    elif size/SizeScale.mb <= scale_factor:
      _size /= SizeScale.mb
      size_type = "mb"
    elif size/SizeScale.gb <= scale_factor:
      _size /= SizeScale.gb
      size_type = "gb"
    elif size/SizeScale.tb <= scale_factor:
      _size /= SizeScale.tb
      size_type = "tb"
    elif size/SizeScale.pb >= scale_factor:
      _size /= SizeScale.pb
      size_type = "pb"

    return _size, size_type


class BucketItemRecord(object):
  def __init__(self, bid: List[str] = None, name: str = "", size: SizeScale = None,
               date: str = "????-??-?? ??:??:??", files: int = 0, raw: List = None):
    self.bid = bid
    self.name = name
    self.size = size
    self.date = date
    self.files = files
    self.raw = raw


class FileOut(BytesIO):
  def __init__(self, _id: object, chunk_size: int, size: int, hash_type: str, hash_value: str, f):
    def ex():
      raise NotImplementedError()

    super().__init__()

    self.size = size
    self.chunk_size = chunk_size
    self.hash_type = hash_type
    self.hash_value = hash_value
    self._id = _id

    # overrides
    self.close = f.close
    self.read = f.read
    self.readline = f.readline
    self.tell = f.tell
    self.seek = f.seek

    self.__iter__ = f.__iter__
    self.__enter__ = f.__enter__
    self.__exit__ = f.__exit__
    self.write = lambda: ex()
    self.writelines = lambda: ex()
    self.writable = lambda: False


class FileIn(BytesIO):
  def __init__(self, chunk_size: int, fid: LambdaType = None, hash_value: LambdaType = None, f: BytesIO = None):
    def ex():
      raise NotImplementedError()

    super().__init__()

    self.chunk_size = chunk_size
    if fid is not None:
      self._fid = fid

    if hash_value is not None:
      self._hash_value = hash_value

    if f is not None:
      # overrides
      self.close = f.close
      self.write = f.write
      self.writelines = f.writelines

      self.__enter__ = f.__enter__
      self.__exit__ = f.__exit__

    self.readable = lambda: False
    self.read = lambda: ex()
    self.read1 = lambda: ex()
    self.readinto = lambda: ex()
    self.readinto1 = lambda: ex()
    self.readline = lambda: ex()
    self.readlines = lambda: ex()

  @property
  def hash_value(self):
    return self._hash_value()

  @property
  def _id(self):
    return self._fid()


class FileItemRecord(object):
  def __init__(self, fid: object or None, filename: str, flen: SizeScale, upload_date: datetime, hash_value: str, f: FileOut):
    self.fid = fid
    self.filename = filename
    self.file_len = flen
    self.upload_date = upload_date
    self.file_hash_value = hash_value
    self.stream = f

  def __str__(self):
    return "{3:32} f {1:7.2f} {4} {2:%Y-%m-%d %H:%M:%S} {0}".format(
      self.filename,
      self.file_len.size,
      self.upload_date,
      self.file_hash_value,
      self.file_len.scale_name)


class StorageCredentials(object):
  user = None
  password = None

  def __init__(self, user, password):
    self.user = user
    self.password = password

  @staticmethod
  def from_string(creds):
    return StorageCredentials(*creds.split(":"))


class GenericStorage(object):

  def __init__(self, host: str, port: str, bucket: str, auth: StorageCredentials, options: Dict[str, str]):
    self._host = host
    self._port = port
    self._database = bucket
    self._auth = auth
    self._options = options

  @classmethod
  def name(cls) -> List[str]:
    raise NotImplementedError()

  def connect(self):
    raise NotImplementedError()

  def disconnect(self):
    raise NotImplementedError()

  def bucket_list(self) -> Iterable[BucketItemRecord]:
    raise NotImplementedError()

  def bucket_exists(self, name: str) -> bool:
    raise NotImplementedError()

  def drop_bucket(self, name: str):
    raise NotImplementedError()

  def list(self, bucket: str, filename: str or None = None) -> Iterable[FileItemRecord]:
    raise NotImplementedError()

  def new_file(self, bucket: str, filename: str) -> FileIn:
    raise NotImplementedError()

  def delete(self, bucket: str, f: FileItemRecord or object):
    raise NotImplementedError()

  def stat(self):
    raise NotImplementedError()

  def server_stats(self, scale_factor: SizeScale) -> Dict[str, str]:
    raise NotImplementedError()

#!/usr/bin/env python3

import gridfs
import os
import sys

import hashlib

from threading import Thread
from enum import Enum

from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase
from pymongo.errors import ConfigurationError

# ====================================== CLASSES ==========================================


class GridFSTable(Enum):
  files = "files"
  chunks = "chunks"


class Metaclass(object):
  def __init__(self, meta_type):
    self._meta_type = meta_type

  def __call__(self, base_class):
    return self._meta_type(base_class.__name__, base_class.__bases__, dict(base_class.__dict__))


class PropertiesMeta(type):
  def __str__(self):
    return ", ".join(self.__dict__[item] for item in self.__dict__ if item[:1] != "_"
                     and isinstance(self.__dict__[item], str))


class Context(object):
  def __init__(self, client=None, db=None, fs=None, args=None, bucket_name=None):
    """
    :type client MongoClient
    :type db MongoDatabase
    :type fs gridfs.GridFS
    :type args list
    :type bucket_name str
    """
    self.client = client
    self.db = db
    self.fs = fs
    self.args = args
    self.bucket_name = bucket_name


class SizeScale(object):
  bytes = 1.0
  kb = bytes * 1024
  mb = kb * 1024
  gb = mb * 1024
  tb = gb * 1024
  pb = tb * 1024


@Metaclass(PropertiesMeta)
class StorageCommands(object):
  list_cmd = "ls"
  stats_cmd = "stats"
  put_cmd = "put"
  get_cmd = "get"
  del_cmd = "rm"

  bucket_consumers = [list_cmd, put_cmd, get_cmd, del_cmd]


@Metaclass(PropertiesMeta)
class BucketCommands(object):
  list_cmd = StorageCommands.list_cmd
  new_cmd = "new"
  delete_cmd = "rm"


def scale_file_size(size):
  """
  Scales file size according to passed scale rate and returns scaled number and scale name

  :type size int|float

  :rtype set(float, str)
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


# ====================================== COMMANDS ==========================================

def get_buckets(ctx):
  """
  :type ctx Context
  """
  buckets = {}
  for collection in ctx.db.list_collections():
    raw_name = collection["name"]
    """:type raw_name str"""
    try:
      record_type = GridFSTable(raw_name.rpartition(".")[2])
    except ValueError:
      record_type = None

    col_stats = ctx.db.command("collStats", raw_name, scale=SizeScale.bytes)
    name, _, _ = collection["name"].rpartition(".")

    if name not in buckets:
      buckets[name] = {
        "size": 0,
        "size_type": " b",
        "files": 0,
        "date": "????-??-?? ??:??:??",
        "raw": []
      }

    bucket = buckets[name]
    if record_type == GridFSTable.chunks:
      bucket["size"] += col_stats["size"]
    elif record_type == GridFSTable.files:
      bucket["files"] = col_stats["count"]
      if bucket["files"] > 0:
        try:
          collection_upload_time = list(ctx.db.get_collection(raw_name).find(limit=1))[0]["uploadDate"]
          """:type collection_upload_time datetime"""
          bucket["date"] = "{0:%Y-%m-%d %H:%M:%S}".format(collection_upload_time)

        except IndexError:
          pass

    bucket["raw"].append(raw_name)

  # re-calculate size scaling
  for bucket in buckets.values():
    bucket["size"], bucket["size_type"] = scale_file_size(bucket["size"])

  return buckets


def list_buckets_command(ctx):
  """
  :type ctx Context
  """
  buckets = get_buckets(ctx)

  for bucket_name, metainfo in buckets.items():
    print(f"{metainfo['files']:032d} d {metainfo['size']:7.2f} {metainfo['size_type']} {metainfo['date']} {bucket_name}")


def bucket_command(ctx):
  """
  args: check BucketCommands

  BucketCommands.delete_cmd:
    args: name

  :type ctx Context
  """
  try:
    command = ctx.args.pop(0)
  except IndexError:
    raise ValueError("Supported bucket commands: {}".format(BucketCommands))

  if command == BucketCommands.list_cmd:
    list_buckets_command(ctx)
  elif command == BucketCommands.new_cmd:
    raise ValueError("No creation needed, just put file in the desired bucket. It will be created if not exists")
  elif command == BucketCommands.delete_cmd:
    try:
      name = ctx.args.pop(0)
    except IndexError:
      raise ValueError("No bucket name passed")

    bucket = get_buckets(ctx)
    if name not in bucket:
      raise ValueError("No bucket with name '{}' found".format(name))

    raw_names = bucket[name]['raw']
    for raw_name in raw_names:
      ctx.db.drop_collection(raw_name)

  else:
    raise ValueError("Unknown command '{}'".format(command))


def list_cmd(ctx):
  """
  optional: bucket name

  :type ctx Context
  """
  if not ctx.fs:
    list_buckets_command(ctx)
    return

  for f in ctx.fs.find():
    flen, flen_type = scale_file_size(f.length)
    print("{3:32} f {1:7.2f} {4} {2:%Y-%m-%d %H:%M:%S} {0}".format(
      f.filename, flen, f.upload_date, f.md5, flen_type))


def print_io_status(filename, total_sz, f_obj):
  """
  :type filename str
  :type total_sz int
  :type f_obj io.BytesIO
  """
  from time import time, sleep

  threshold = 0.3

  current_sz = 0
  total_sz = float(total_sz)
  total_sz_scale, total_sz_name = scale_file_size(total_sz)
  time_watched_prev = time()
  prev_sz = f_obj.tell()
  speed = 0
  speed_n = " b"

  while not f_obj.closed and current_sz < total_sz:
    current_sz = f_obj.tell()

    time_watched = time()
    time_delta = time_watched - time_watched_prev
    if time_delta >= threshold:
      speed, speed_n = scale_file_size((current_sz - prev_sz) / time_delta)
      time_watched_prev = time_watched
      prev_sz = current_sz

    left_percents = round((current_sz / total_sz) * 100)
    current_sz_scale, curr_sz_type = scale_file_size(current_sz)

    sys.stdout.write(f"\r{filename} --> {current_sz_scale:>6.2f} {curr_sz_type}/{total_sz_scale:.2f} {total_sz_name} ({left_percents:3d}%) {speed:6.2f}{speed_n}/s")
    sleep(0.15)


def put_cmd(ctx):
  """
  args: bucket name, source
  optional: filename

  :type ctx Context
  """
  try:
    source = ctx.args.pop(0)
    if not source:
      raise ValueError("No source filename passed")
  except (IndexError, ValueError):
    raise

  full_file_path = os.path.abspath(source)

  try:
    filename = ctx.args.pop(0)
  except IndexError:
    filename = os.path.basename(full_file_path)

  hash = hashlib.md5()

  if not os.path.exists(full_file_path):
    raise ValueError("Source file '{}' not found".format(full_file_path))

  with open(full_file_path, "br") as f:
    fsize = os.path.getsize(full_file_path)
    th = Thread(target=print_io_status, args=(filename, fsize, f))
    th.start()

    stream = ctx.fs.new_file(filename=filename)

    chunk_size = stream.chunk_size
    total_chunked_size = int(fsize / chunk_size) * chunk_size
    read_left_size = fsize - total_chunked_size

    assert total_chunked_size + read_left_size == fsize

    while f.tell() < total_chunked_size:
      try:
        chunk = f.read(chunk_size)
        stream.write(chunk)
        hash.update(chunk)
      except IOError:
        f.close()
        stream.close()
        raise

    if read_left_size > 0:
      try:
        chunk = f.read(read_left_size)
        stream.write(chunk)
        hash.update(chunk)
      except IOError:
        f.close()
        stream.close()
        raise

    stream.close()
    th.join()

  sys.stdout.write("\r" + " "*80)
  if stream.md5 != hash.hexdigest():
    ctx.fs.delete(stream._id)
    print("\r{}".format("failed, local hash didn't match server one"))
  else:
    for f in ctx.fs.find({'filename': filename}):  # remove previous versions of the file
      if f._id != stream._id:
        ctx.fs.delete(f._id)

    print("\r{}".format("{} transferred".format(filename)))


def del_cmd(ctx):
  """
  args: bucket name, filename

  :type ctx Context
  """
  try:
    filename = ctx.args.pop(0)
  except (IndexError, ValueError):
    ctx.args = [BucketCommands.delete_cmd, ctx.bucket_name]
    bucket_command(ctx)
    return

  files = list(ctx.fs.find({'filename': filename}))

  if not files:
    raise ValueError("Filename '{}' not found in the storage".format(filename))

  ctx.fs.delete(files[0]._id)


def get_cmd(ctx):
  """
  args: bucket name, filename
  optional: destination

  :type ctx Context
  """
  try:
    filename = ctx.args.pop(0)
  except (IndexError, ValueError):
    filename = None

  try:
    if not filename:
      raise IndexError()
    destination = ctx.args.pop(0)
  except (IndexError, ValueError):
    destination = "."

  files = list(ctx.fs.find({'filename': filename}) if filename else fs.find())

  if not files:
    raise ValueError("Filename '{}' not found in the storage".format(filename))

  for stream in files:
    stream = stream
    """:type stream GridOut"""
    dest = os.path.abspath(destination)
    if os.path.isdir(dest) and dest[-1:] != os.path.sep:
      dest = dest + os.path.sep

    if not os.path.basename(dest):
      dest = os.path.join(dest, stream.filename)

    if os.path.exists(dest):
      os.remove(dest)

    hash = hashlib.md5()

    with open(dest, "bw") as f:
      th = Thread(target=print_io_status, args=(stream.filename, stream.length, f))
      th.start()

      chunk_size = stream.chunk_size
      total_chunked_size = int(stream.length / chunk_size) * chunk_size
      read_left_size = stream.length - total_chunked_size

      assert total_chunked_size + read_left_size == stream.length

      while f.tell() < total_chunked_size:
        try:
          chunk = stream.read(chunk_size)
          f.write(chunk)
          hash.update(chunk)
        except IOError:
          f.close()  # to end watching thread
          raise

      if read_left_size > 0:
        try:
          chunk = stream.read(read_left_size)
          f.write(chunk)
          hash.update(chunk)
        except IOError:
          f.close()
          raise

      th.join()

    sys.stdout.write("\r" + " "*80)
    if stream.md5 != hash.hexdigest():
      print("\r{}".format("failed, local hash didn't match server one"))
    else:
      print("\r{}".format("{} transferred".format(os.path.basename(dest))))


def stats_cmd(ctx):
  """
   args: none

  :type ctx Context
  """
  db_version = ctx.client.server_info()["version"]
  db_stats = ctx.db.command("dbStats", 1, scale=SizeScale.mb)

  db_name = db_stats["db"] if db_stats and "db" in db_stats else "<unknown>"
  storage_size = db_stats["storageSize"] if db_stats and "storageSize" in db_stats else -1
  data_size = db_stats["dataSize"] if db_stats and "dataSize" in db_stats else -1
  collections_count = db_stats["collections"] if db_stats and "collections" in db_stats else -1

  print("""Server version {0}\n
db          : {1}
storage size: {2:.2f} Mb
data size   : {3:.2f} Mb
collections : {4}\n""".format(db_version, db_name, storage_size, data_size, collections_count))


# ====================================== BASE STUFF ==========================================
AVAILABLE_COMMANDS = {
  StorageCommands.list_cmd: list_cmd,
  StorageCommands.put_cmd: put_cmd,
  StorageCommands.get_cmd: get_cmd,
  StorageCommands.del_cmd: del_cmd,
  StorageCommands.stats_cmd: stats_cmd
}


def main(args):
  mongodb_url = os.environ.get("MONGODB_URL", None)
  if not mongodb_url:
    raise ValueError("MONGODB_URL environment variable should be set to value like 'mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]'")

  try:
    command = args.pop(0)
  except IndexError:
    raise ValueError("No command provided!")

  mongodb = MongoClient(mongodb_url)
  try:
    db = mongodb.get_database()
  except ConfigurationError as e:
    raise ValueError("Please check configuration url: {}".format(str(e)))

  bucket = None

  if command in StorageCommands.bucket_consumers:  # if yes, second argument should be bucket name
    try:
      bucket = args.pop(0)

      if os.path.exists(bucket) and len(args) == 0:
        raise ValueError("You can't pass filename here, please supply properly formatted command after consulting with help")
    except IndexError:
      command = StorageCommands.list_cmd
      bucket = None

    if bucket and command != StorageCommands.put_cmd:
      buckets = {item.rpartition(".")[0] for item in db.list_collection_names()}
      if bucket not in buckets:
        raise ValueError("No such bucket '{}' found, check full list of available buckets using list command with no options". format(bucket))

  fs = gridfs.GridFS(db, collection=str(bucket)) if bucket else None

  ctx = Context(client=mongodb, db=db, fs=fs, bucket_name=bucket, args=args)

  if command in AVAILABLE_COMMANDS:
    AVAILABLE_COMMANDS[command](ctx)
  else:
    raise ValueError("Unknown command '{}'".format(command))


def help_command():

  print("""Command line description:
    {0} <command> <bucket name> options1...optionsN
  """.format(os.path.basename(__file__)))

  print("Supported commands: {}".format(StorageCommands))


if __name__ == "__main__":
  args = list(sys.argv)
  args.pop(0)

  if not args:
    help_command()
  else:
    try:
      main(args)
    except ValueError as e:
      print(str(e))
      sys.exit(-1)
    except Exception:
     if "pydev" in os.environ.get("PYTHONPATH", ""):  # do not handle it if we running from IDE
       raise

     import traceback
     track = traceback.format_exc()
     print("////// TRACKBACK")
     print(track)
     print("/////  END")
     print("\n\n")
     help_command()
     sys.exit(-1)



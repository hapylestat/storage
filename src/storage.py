#!/usr/bin/env python3

import gridfs
import os
import sys

import time
import hashlib
from threading import Thread


from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase
from pymongo.errors import ConfigurationError

# ====================================== CLASSES ==========================================


class Metaclass(object):
  def __init__(self, meta_type):
    self._meta_type = meta_type

  def __call__(self, base_class):
    return self._meta_type(base_class.__name__, base_class.__bases__, dict(base_class.__dict__))


class PropertiesMeta(type):
  def __str__(self):
    return ", ".join(self.__dict__[item] for item in self.__dict__ if item[:1] != "_"
                     and isinstance(self.__dict__[item], str))


class SizeScale(object):
  mb = 1027*1024


@Metaclass(PropertiesMeta)
class StorageCommands(object):
  list_cmd = "ls"
  stats_cmd = "stats"
  bucket_cmd = "bucket"
  put_cmd = "put"
  get_cmd = "get"
  del_cmd = "del"

  bucket_consumers = [list_cmd, put_cmd, get_cmd, del_cmd]


@Metaclass(PropertiesMeta)
class BucketCommands(object):
  list_cmd = StorageCommands.list_cmd
  new_cmd = "new"
  delete_cmd = "del"


# ====================================== COMMANDS ==========================================

def get_buckets(db):
  """
  :type db MongoDatabase
  """
  buckets = {}
  for collection in db.list_collections():
    raw_name = collection["name"]
    name, _, _ = collection["name"].rpartition(".")

    if name not in buckets:
      buckets[name] = {
        'size': 0,
        'raw': []
      }

    buckets[name]['size'] += int(db.command("collStats", raw_name, scale=SizeScale.mb)["size"])
    buckets[name]['raw'].append(raw_name)

  return buckets


def list_buckets_command(db):
  """
  :type db MongoDatabase
  """
  buckets = get_buckets(db)

  for bucket_name, bucket_size in buckets.items():
    print("{0:20}   {1:10} Mb".format(bucket_name, bucket_size['size']))


def bucket_command(db, args):
  """
  args: check BucketCommands

  BucketCommands.delete_cmd:
    args: name

  :type db MongoDatabase
  :type args list
  """
  try:
    command = args.pop(0)
  except IndexError:
    raise ValueError("Supported bucket commands: {}".format(BucketCommands))

  if command == BucketCommands.list_cmd:
    list_buckets_command(db)
  elif command == BucketCommands.new_cmd:
    raise ValueError("No creation needed, just put file in the desired bucket. It will be created if not exists")
  elif command == BucketCommands.delete_cmd:
    try:
      name = args.pop(0)
    except IndexError:
      raise ValueError("No bucket name passed")

    bucket = get_buckets(db)
    if name not in bucket:
      raise ValueError("No bucket with name '{}' found".format(name))

    raw_names = bucket[name]['raw']
    for raw_name in raw_names:
      db.drop_collection(raw_name)

  else:
    raise ValueError("Unknown command '{}'".format(command))


def list_cmd(db, fs, args):
  """
  optional: bucket name

  :type db MongoDatabase
  :type fs gridfs.GridFS
  :type args list
  """
  if not fs:
    list_buckets_command(db)
    return

  for f in fs.find():
    print("{3:33}  {1:10.2f} Mb  {2:%Y-%m-%d %H:%M:%S}   {0:>30}".format(
      f.filename, f.length / SizeScale.mb, f.upload_date, f.md5))


def print_io_status(filename, total_size, f_obj):
  """
  :type filename str
  :type total_size int
  :type f_obj io.BytesIO
  """

  current_pos = 0
  total_size = float(total_size)
  while not f_obj.closed and current_pos < total_size:
    current_pos = f_obj.tell()
    left_percents = round((current_pos / total_size) * 100)

    sys.stdout.write("\r{0} --> {1:.2f}/{2:.2f} Mb ({3:3d}%)".format(
      filename,
      current_pos/SizeScale.mb,
      total_size/SizeScale.mb,
      left_percents))
    time.sleep(0.3)


def put_cmd(db, fs, args):
  """
  args: bucket name, source
  optional: filename

  :type db MongoDatabase
  :type fs gridfs.GridFS
  :type args list
  """
  try:
    source = args.pop(0)
    if not source:
      raise ValueError("No source filename passed")
  except (IndexError, ValueError):
    raise

  full_file_path = os.path.abspath(source)

  try:
    filename = args.pop(0)
  except IndexError:
    filename = os.path.basename(full_file_path)

  hash = hashlib.md5()

  if not os.path.exists(full_file_path):
    raise ValueError("Source file '{}' not found".format(full_file_path))

  with open(full_file_path, "br") as f:
    fsize = os.path.getsize(full_file_path)
    th = Thread(target=print_io_status, args=(filename, fsize, f))
    th.start()

    stream = fs.new_file(filename=filename)

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
    fs.delete(stream._id)
    print("\r{}".format("failed, local hash didn't match server one"))
  else:
    for f in fs.find({'filename': filename}):  # remove previous versions of the file
      if f._id != stream._id:
        fs.delete(f._id)

    print("\r{}".format("{} transferred".format(filename)))


def del_cmd(db, fs, args):
  """
  args: bucket name, filename

  :type db MongoDatabase
  :type fs gridfs.GridFS
  :type args list
  """
  try:
    filename = args.pop(0)
  except (IndexError, ValueError):
    raise ValueError("No filename passed")

  files = list(fs.find({'filename': filename}))

  if not files:
    raise ValueError("Filename '{}' not found in the storage".format(filename))

  fs.delete(files[0]._id)


def get_cmd(db, fs, args):
  """
  args: bucket name, filename
  optional: destination

  :type db MongoDatabase
  :type fs gridfs.GridFS
  :type args list
  """
  try:
    filename = args.pop(0)
  except (IndexError, ValueError):
    filename = None

  try:
    if not filename:
      raise IndexError()
    destination = args.pop(0)
  except (IndexError, ValueError):
    destination = "."

  files = list(fs.find({'filename': filename}) if filename else fs.find())

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


def stats_cmd(client, db):
  """
   args: none

  :type client MongoClient
  :type db MongoDatabase
  """
  db_version = client.server_info()["version"]
  db_stats = db.command("dbStats", 1, scale=SizeScale.mb)

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
    except IndexError:
      command = StorageCommands.list_cmd
      bucket = None

    if bucket and command != StorageCommands.put_cmd:
      buckets = {item.partition(".")[0] for item in db.list_collection_names()}
      if bucket not in buckets:
        raise ValueError("No such bucket '{}' found, check full list of available buckets using list command with no options". format(bucket))

  fs = gridfs.GridFS(db, collection=str(bucket)) if bucket else None

  if command == StorageCommands.list_cmd:
    list_cmd(db, fs, args)
  elif command == StorageCommands.stats_cmd:
    stats_cmd(mongodb, db)
  elif command == StorageCommands.bucket_cmd:
    bucket_command(db, args)
  elif command == StorageCommands.put_cmd:
    put_cmd(db, fs, args)
  elif command == StorageCommands.get_cmd:
    get_cmd(db, fs, args)
  elif command == StorageCommands.del_cmd:
    del_cmd(db, fs, args)
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



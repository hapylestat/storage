#!/usr/bin/env python3

import os
import sys
import hashlib

from threading import Thread

# ====================================== CLASSES ==========================================
from storage.internal.configuration import Configuration
from storage.internal.storages import StorageFactory, GenericStorage
from storage.internal.storages.generic import SizeScale


__version__ = "1.0.1b1"


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
  def __init__(self, db=None, args=None, bucket_name=None):
    """
    :type db GenericStorage
    :type args list
    :type bucket_name str
    """
    self.db = db
    self.args = args
    self.bucket_name = bucket_name


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


# ====================================== COMMANDS ==========================================

def list_buckets_command(ctx):
  """
  :type ctx Context
  """
  buckets = ctx.db.bucket_list()

  for bucket in buckets:
    if not bucket.size:
      bucket.size = SizeScale(0)
    print(f"{bucket.files:032d} d {bucket.size.size:7.2f} {bucket.size.scale_name} {bucket.date} {bucket.name}")


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
    raise ValueError("Supported database commands: {}".format(BucketCommands))

  if command == BucketCommands.list_cmd:
    list_buckets_command(ctx)
  elif command == BucketCommands.new_cmd:
    raise ValueError("No creation needed, just put file in the desired database. It will be created if not exists")
  elif command == BucketCommands.delete_cmd:
    try:
      name = ctx.args.pop(0)
    except IndexError:
      raise ValueError("No database name passed")

    if not ctx.db.bucket_exists(name):
      raise ValueError("No database with name '{}' found".format(name))

    ctx.db.drop_bucket(name)
  else:
    raise ValueError("Unknown command '{}'".format(command))


def list_cmd(ctx):
  """
  optional: database name

  :type ctx Context
  """
  if not ctx.bucket_name:
    list_buckets_command(ctx)
    return

  for f in ctx.db.list(ctx.bucket_name):
    print(f)


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
  total_sz_scale, total_sz_name = SizeScale.scale(total_sz)
  time_watched_prev = time()
  prev_sz = f_obj.tell()
  speed = 0
  speed_n = " b"

  while not f_obj.closed and current_sz < total_sz:
    current_sz = f_obj.tell()

    time_watched = time()
    time_delta = time_watched - time_watched_prev
    if time_delta >= threshold:
      speed, speed_n = SizeScale.scale((current_sz - prev_sz) / time_delta)
      time_watched_prev = time_watched
      prev_sz = current_sz

    left_percents = round((current_sz / total_sz) * 100)
    current_sz_scale, curr_sz_type = SizeScale.scale(current_sz)

    sys.stdout.write(f"\r{filename} --> {current_sz_scale:>6.2f} {curr_sz_type}/{total_sz_scale:.2f} {total_sz_name} ({left_percents:3d}%) {speed:6.2f}{speed_n}/s")
    sleep(0.15)


def put_cmd(ctx):
  """
  args: database name, source
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

    stream = ctx.db.new_file(ctx.bucket_name, filename)

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
  if stream.hash_value != hash.hexdigest():
    ctx.db.delete(ctx.bucket_name, stream._id)
    print("\r{}".format("failed, local hash_value didn't match server one"))
  else:
    for f in ctx.db.list(ctx.bucket_name, filename):  # remove previous versions of the file
      if f.fid != stream._id:
        ctx.db.delete(ctx.bucket_name, f.fid)

    print("\r{}".format("{} transferred".format(filename)))


def del_cmd(ctx):
  """
  args: database name, filename

  :type ctx Context
  """
  try:
    filename = ctx.args.pop(0)
  except (IndexError, ValueError):
    ctx.args = [BucketCommands.delete_cmd, ctx.bucket_name]
    bucket_command(ctx)
    return

  files = ctx.db.list(ctx.bucket_name, filename)

  if not files:
    raise ValueError("Filename '{}' not found in the storage".format(filename))

  for f in files:
    ctx.db.delete(ctx.bucket_name, f)


def get_cmd(ctx):
  """
  args: database name, filename
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

  files = list(ctx.db.list(ctx.bucket_name, filename))

  if not files:
    raise ValueError("Filename '{}' not found in the storage".format(filename))

  for f_record in files:
    dest = os.path.abspath(destination)
    if os.path.isdir(dest) and dest[-1:] != os.path.sep:
      dest = dest + os.path.sep

    if not os.path.basename(dest):
      dest = os.path.join(dest, f_record.filename)

    if os.path.exists(dest):
      os.remove(dest)

    supported_hashes = {
      "md5": hashlib.md5,
      "sha256": hashlib.sha256
    }
    hash = supported_hashes[f_record.stream.hash_type]()

    with open(dest, "bw") as f:
      th = Thread(target=print_io_status, args=(f_record.filename, f_record.stream.size, f))
      th.start()

      chunk_size = f_record.stream.chunk_size
      total_chunked_size = int(f_record.stream.size / chunk_size) * chunk_size
      read_left_size = f_record.stream.size - total_chunked_size

      assert total_chunked_size + read_left_size == f_record.stream.size

      while f.tell() < total_chunked_size:
        try:
          chunk = f_record.stream.read(chunk_size)
          f.write(chunk)
          hash.update(chunk)
        except IOError:
          f.close()  # to end watching thread
          raise

      if read_left_size > 0:
        try:
          chunk = f_record.stream.read(read_left_size)
          f.write(chunk)
          hash.update(chunk)
        except IOError:
          f.close()
          raise

      th.join()

    sys.stdout.write("\r" + " " * 80)
    if f_record.stream.hash_value != hash.hexdigest():
      print("\r{}".format("failed, local hash_value didn't match server one"))
    else:
      print("\r{}".format("{} transferred".format(os.path.basename(dest))))


def stats_cmd(ctx):
  """
   args: none

  :type ctx Context
  """
  server_stats = ctx.db.server_stats(SizeScale.bytes)

  max_key_len = 0
  for k in server_stats.keys():
    if max_key_len < len(k):
      max_key_len = len(k)

  for k, v in server_stats.items():
    fill_size = max_key_len - len(k)
    print("{}{}: {}".format(k, " " * fill_size, v))


def select_storage(cfg: Configuration):
  url_list = cfg.connection_url_list

  for i in range(0, len(url_list)):
    print("{}. {}".format(i, url_list[i]))

  print("--------")
  i = input("Please choose: ")
  try:
    i = int(i)
    if i < len(url_list):
      cfg.connection_url_index = i
      print("/// OK")
    else:
      raise TypeError("Too big index")
  except TypeError as e:
    print("Error. {}".format(e))


# ====================================== BASE STUFF ==========================================
AVAILABLE_COMMANDS = {
  StorageCommands.list_cmd: list_cmd,
  StorageCommands.put_cmd: put_cmd,
  StorageCommands.get_cmd: get_cmd,
  StorageCommands.del_cmd: del_cmd,
  StorageCommands.stats_cmd: stats_cmd
}


def main(args):
  cfg = Configuration()

  if not cfg.connection_url:
    raise ValueError("STORAGE_URL environment variable or configuration file storage.conf in user directory"
                     " should be set to value like 'storage://[username:password@]host1[:port1][,host2[:port2]"
                     ",...[,hostN[:portN]]][/[database][?options]]'")

  try:
    command = args.pop(0)
  except IndexError:
    raise ValueError("No command provided!")

  if command == "conn":  # ToDo: integrate command more nicely
    select_storage(cfg)
    return

  bucket = None
  db = StorageFactory.get(cfg.connection_url)
  db.connect()

  if command in StorageCommands.bucket_consumers:  # if yes, second argument should be database name
    try:
      bucket = args.pop(0)

      if os.path.exists(bucket) and len(args) == 0:
        raise ValueError("You can't pass filename here, please supply properly formatted command after consulting with help")
    except IndexError:
      command = StorageCommands.list_cmd
      bucket = None

    if bucket and command != StorageCommands.put_cmd:
      if not db.bucket_exists(bucket):
        raise ValueError("No such bucket '{}' found, check full list of available buckets using list command with no options". format(bucket))

  ctx = Context(db=db, bucket_name=bucket, args=args)

  if command in AVAILABLE_COMMANDS:
    AVAILABLE_COMMANDS[command](ctx)
  else:
    raise ValueError("Unknown command '{}'".format(command))


def help_command():

  print("""Command line description:
    {0} <command> <database name> options1...optionsN
  """.format(os.path.basename(__file__)))

  print("Supported commands: {}".format(StorageCommands))


def main_entry():
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


if __name__ == "__main__":
  main_entry()



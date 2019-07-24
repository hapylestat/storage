
import os
import sys
from typing import List


class Configuration(object):

  def __init__(self):
    self.__connection_urls = []
    self.__connection_url_index = -1

    if sys.platform.startswith('java'):
      import platform
      os_name = platform.java_ver()[3][0]
      if os_name.startswith('Windows'):
        self.__system = 'win32'
      elif os_name.startswith('Mac'):
        self.__system = 'darwin'
      else:
        self.__system = 'linux2'
    else:
      self.__system = sys.platform

    self.__get_connection_url()
    self.__get_connection_index()

  def __user_data_dir(self, appname=None, appauthor=None, version=None):
    if self.__system == "win32":
      if appauthor is None:
        appauthor = appname

      path = os.path.normpath(os.getenv("LOCALAPPDATA", None))
      if appname:
        if appauthor is not False:
          path = os.path.join(path, appauthor, appname)
        else:
          path = os.path.join(path, appname)
    elif self.__system == 'darwin':
      path = os.path.expanduser('~/Library/Application Support/')
      if appname:
        path = os.path.join(path, appname)
    else:
      path = os.getenv('XDG_DATA_HOME', os.path.expanduser("~/.local/share"))
      if appname:
        path = os.path.join(path, appname)
    if appname and version:
      path = os.path.join(path, version)

    return path

  def __user_config_dir(self, appname=None, appauthor=None, version=None):
    if self.__system == "win32":
      path = self.__user_data_dir(appname, appauthor, None)
    elif self.__system == 'darwin':
      path = os.path.expanduser('~/Library/Preferences/')
      if appname:
        path = os.path.join(path, appname)
    else:
      path = os.getenv('XDG_CONFIG_HOME', os.path.expanduser("~/.config"))
      if appname:
        path = os.path.join(path, appname)
    if appname and version:
      path = os.path.join(path, version)
    return path

  def __get_connection_url(self):
    url = os.environ.get("STORAGE_URL", None)
    if url:
      self.__connection_urls = [url]
      self.__connection_url_index = 0
      return

    config = os.path.join(self.__user_config_dir("storage", appauthor=False), "storage.conf")
    try:
      with open(config, 'r') as f:
        self.__connection_urls = [line.strip(os.linesep) for line in f.readlines()]
    except IOError:
      self.__connection_urls = []
      return

  def __get_connection_index(self):
    url = os.environ.get("STORAGE_URL", None)
    if url:
      return 0

    config = os.path.join(self.__user_config_dir("storage", appauthor=False), "storage.setting")
    try:
      with open(config, 'r') as f:
        self.__connection_url_index = int(f.readline().strip(os.linesep))
    except IOError:
      self.__connection_url_index = 0

  def __set_connection_index(self, value: int):
    config = os.path.join(self.__user_config_dir("storage", appauthor=False), "storage.setting")
    try:
      with open(config, 'w') as f:
        f.write(str(value))
      self.__connection_url_index = value
    except IOError:
      self.__connection_url_index = 0

  @property
  def connection_url_list(self) -> List[str]:
    return self.__connection_urls

  @property
  def connection_url(self) -> str:
    return self.__connection_urls[self.__connection_url_index]

  @property
  def connection_url_index(self) -> int:
    return self.__connection_url_index

  @connection_url_index.setter
  def connection_url_index(self, value: int):
    self.__set_connection_index(value)

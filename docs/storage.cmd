@echo off

IF NOT DEFINED MONGODB_URL (set MONGODB_URL=mongodb://default.host:27000/db)


C:\Python\Python36\python.exe storage.py %*

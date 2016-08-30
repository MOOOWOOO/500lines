# This started as a very thin wrapper around a file object, with intent to
# provide an object address on write() and a superblock. But as I was writing
# it, I realised that the user really wouldn't want to deal with the lengths of
# the writen chunks (and Pickle won't do it for you), so this module would have
# to abstract the file object into it's own degenerate key/value store.
# (Degenerate because you can't pick the keys, and it never releases storage,
# even when it becomes unreachable!)

import os
import struct

import portalocker


class Storage(object):
    superblock_size = 4096
    integer_format = "!Q"
    integer_length = 8

    def __init__(self, dbfile):
        self._dbfile = dbfile
        self.locked = False
        self._ensure_superblock()

    def _ensure_superblock(self):
        self.lock()
        self._seek_end()
        end_address = self._dbfile.tell()
        if end_address < self.superblock_size:
            self._dbfile.write(b'\x00' * (self.superblock_size - end_address))
        self.unlock()

    def lock(self):
        if not self.locked:
            portalocker.lock(self._dbfile, portalocker.LOCK_EX)
            self.locked = True
            return True
        else:
            return False

    def unlock(self):
        if self.locked:
            self._dbfile.flush()
            portalocker.unlock(self._dbfile)
            self.locked = False

    def _seek_end(self):
        self._dbfile.seek(0, os.SEEK_END)

    def _seek_superblock(self):
        self._dbfile.seek(0)

    def _bytes_to_integer(self, integer_bytes):
        return struct.unpack(self.integer_format, integer_bytes)[0]

    def _integer_to_bytes(self, integer):
        return struct.pack(self.integer_format, integer)

    def _read_integer(self):
        return self._bytes_to_integer(self._dbfile.read(self.integer_length))

    def _write_integer(self, integer):
        self.lock()
        self._dbfile.write(self._integer_to_bytes(integer))

    def write(self, data):
        self.lock()
        self._seek_end()
        object_address = self._dbfile.tell()
        self._write_integer(len(data))
        self._dbfile.write(data)
        return object_address

    def read(self, address):
        self._dbfile.seek(address)
        length = self._read_integer()
        data = self._dbfile.read(length)
        return data

    def commit_root_address(self, root_address):
        self.lock()
        self._dbfile.flush()
        self._seek_superblock()
        self._write_integer(root_address)
        self._dbfile.flush()
        self.unlock()

    def get_root_address(self):
        self._seek_superblock()
        root_address = self._read_integer()
        return root_address

    def close(self):
        self.unlock()
        self._dbfile.close()

    @property
    def closed(self):
        return self._dbfile.closed

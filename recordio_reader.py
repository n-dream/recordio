#!/usr/bin/env python
#
# Copyright 2014 Andrin von Rechenberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from recordio.recordio_shard import RecordIOShard, RecordIOShardDoesNotExistError, SPLIT_CHAR, SPLIT_CHAR_AFTER
from recordio.recordio_records import RecordIORecords
from recordio import recordio_entry_types

import marshal
import cPickle

class RecordIOReader():
  """This class allows you to read data from a RecordIO."""
  def __init__(self, name):
    """Creates a RecordIOReader

    :param name: The name of the RecordIO.
    """
    self.name = name
    self.not_read = []
    self.db_search_and_get = 0

  def __getitem__(self, key):
    """Returns a value of a key. More expensive than reading sequentially.

    :param key: The key of the value to be returned.
    :return: The value.
    """
    for entry_key, value in self.read(key):
      if entry_key != key:
        raise KeyError(key)
      return value
    raise KeyError(key)
  
  def __iter__(self):
    """Iterates of all key, values in this RecordIO

    :return: Yields key, values
    """
    for key, value in self.read():
      yield key, value
  
  def __contains__(self, key):
    """Checks whether an key exists in a RecordIO.

    :param key: The key name
    :return: Boolean
    """
    try:
      self[key]
      return True
    except:
      return False

  def read(self, start_key="", end_key=None, limit=None):
    """Returns all (key, values) in a given range.

    :param start_key: (String) The start key.
    :param end_key: (String) The end key. Exclusive.
    :param limit: (Integer) The maximum amount of entries to be read.
    """
    count = 0
    for key, value in self.read_typed_(start_key, end_key):
      if value[0] == recordio_entry_types.STRING:
        yield key, value[1:]
      elif value[0] == recordio_entry_types.MARSHAL:
        yield key, marshal.loads(value[1:])
      elif value[0] == recordio_entry_types.CPICKLE:
        yield key, cPickle.loads(value[1:])
      else:
        raise ValueError()
      count += 1
      if limit != None and count >= limit:
        break

  @staticmethod
  def all_names():
    """Returns the names of all existing RecordIOs.

    :return: list of RecordIO names
    """
    for x in RecordIOShard.all(keys_only=True).filter("index =", True):
      yield RecordIOShard.get_name(x.name())

  def get_not_read(self):
    """Returns a list of keys of element that were not yet completely written.

    :return: list of keys
    """
    result = self.not_read
    self.not_read = []
    return result

  def db_stats(self):
    """Returns some datastore access statistics.

    :return: Dict
    """
    return { "search_and_get": self.db_search_and_get }

  def read_typed_(self, start_key="", end_key=None):
    """An internal helper function to join split entries.

    :param start_key: An entry tuple (no value needed)
    :param end_key: An entry tuple (no value needed) Exclusive.
    :return: Yields key, value
    """
    if end_key == "":
      return
    start_key_entry = None
    if start_key:
      if isinstance(start_key, unicode):
        try:
          start_key = str(start_key)
        except:
          pass
      if not isinstance(start_key, str):
        raise ValueError("start must be <type 'str'> got: %s" % type(start_key))
      start_key_entry = (start_key, )
    end_key_entry = None
    if end_key:
      if isinstance(end_key, unicode):
        try:
          end_key = str(end_key)
        except:
          pass
      if not isinstance(end_key, str):
        raise ValueError("end must be <type 'str'> got: %s" % type(end_key))
      end_key_entry = (end_key, )
    
    split_entry = []
    for entry in self.read_entries_(start_key_entry, end_key_entry):
      if len(entry) == 2:
        if split_entry:
          self.not_read.append(split_entry[0][0])
        yield entry
      elif len(entry) == 5:
        if entry[1] == 0:
          if split_entry:
            self.not_read.append(split_entry[0][0])
          split_entry = [entry]
        elif (split_entry and split_entry[0][0] == entry[0] and
              len(split_entry) == int(entry[1]) and
              split_entry[0][3] == entry[3]):
          split_entry.append(entry)
        if split_entry and len(split_entry) == int(split_entry[0][2]):
          value = "".join([x[4] for x in split_entry])
          yield entry[0], value
          split_entry = []
    
  def read_entries_(self, start_key=None, end_key=None):
    """An internal helper function to read split entries.

    :param start_key: An entry tuple (no value needed)
    :param end_key: An entry tuple (no value needed) Exclusive.
    :return: Yields key, split_values
    """
    # TODO (andrin): fetch a couple of shards instead of just one based on
    #                method argument
    current_key = start_key
    if current_key == None:
      current_key = ("", )
    limit_shard_name = RecordIOShard.key_name(
        self.name, lo=start_key, hi=end_key).split(SPLIT_CHAR)
    while True:
      shard = RecordIOShard.get_shards_for_key_values(
          self.name, [current_key], keys_only=False).next()[0]
      self.db_search_and_get += 1
      if shard == None:
        raise RecordIOShardDoesNotExistError(self.name)
      hi = shard.lo_hi()[1]
      shard_name = shard.key().name().split(SPLIT_CHAR)
      if (shard_name[6:10] >= limit_shard_name[6:10] and
          (shard_name[2:5] < limit_shard_name[2:5] or
           limit_shard_name[2] == SPLIT_CHAR_AFTER)):
        # Read the whole shard
        for entry in shard:
          yield entry
      else:
        # Read parts of the shard
        for entry in shard.read(current_key, end_key):
          yield entry
      if hi == None:
        # Was the last shard
        return
      current_key = hi
      if (end_key != None and
          RecordIORecords.entry_comperator(current_key, end_key) >= 0):
        # Next shard is after end_key
        return
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

###############################################################################
#                                                                             #
#  An internal RecordIO module. Use RecordIOWriter or RecordIOReader instead! #
#                                                                             #
###############################################################################

from recordio.recordio_records import RecordIORecords
from recordio.recordio_records_zipped import RecordIORecordsZipped
from google.appengine.ext import db

import urllib
import binascii

MAX_BLOB_SIZE = 1000000

# Used for key names, must be lexographically before any hexlified value and
# any urllib.quote'd char
SPLIT_CHAR = "!"
# Used for key names, must be lexographically after any hexlified value and
# any urllib.quote'd char
SPLIT_CHAR_AFTER = "|"
INTEGER_MAX = 9999999999
INTEGER_FMT = "%%0%dd" % len(str(INTEGER_MAX))
INTEGER_FMT_0 = INTEGER_FMT % 0
INTEGER_FMT_1 = INTEGER_FMT % 1

class RecordIOShard(db.Model):
  """Holds the actual data of a RecordIO as sharded datastore entries."""

  # The data from a RecordIORecords or RecordIORecordsZipped
  data = db.BlobProperty()
  # Determines if it's a RecordIORecords or RecordIORecordsZipped.
  compressed = db.BooleanProperty(default=True, indexed=False)
  # The first shard is the index shard. Used for getting a list of all
  # RecordIO names.
  index = db.BooleanProperty()

  @staticmethod
  def entry_key(key):
    """Returns a list of escaped strings representing a RecordIO entry tuple.

    :param key: An entry tuple (no value needed)
    :return: list of escaped string.
    """
    if len(key) >= 4:
      str_key = [INTEGER_FMT % i for i in key[1:4]]
      return (binascii.hexlify(key[0]), ) + tuple(str_key)
    return (binascii.hexlify(key[0]),
            INTEGER_FMT_0, INTEGER_FMT_1, INTEGER_FMT_0)

  @staticmethod
  def key_name(name, lo=None, hi=None):
    """Returns the datastore key name for a shard.

    :param name: The name of the RecordIO
    :param lo: The lo entry tuple.
    :param hi: The hi entry tuple.
    :return: String
    """
    if lo == None:
      lo = ("", INTEGER_FMT_0, INTEGER_FMT_1, INTEGER_FMT_0)
    else:
      lo = RecordIOShard.entry_key(lo)
    if hi == None:
      hi = (SPLIT_CHAR_AFTER, INTEGER_FMT_0, INTEGER_FMT_1, INTEGER_FMT_0)
    else:
      hi = RecordIOShard.entry_key(hi)
    return SPLIT_CHAR.join((urllib.quote(name), "0") + hi + lo)

  @staticmethod
  def create(name, lo=None, hi=None, compressed=True):
    """Creates a new RecordIOShard object (in memory).

    :param name: The name of the RecordIO
    :param lo: The lo entry tuple.
    :param hi: The hi entry tuple.
    :param compressed: Whether this RecordIOs data is zipped or not.
    :return: RecordIOShard
    """
    shard = RecordIOShard(key_name=RecordIOShard.key_name(name, lo, hi))
    shard.compressed = compressed
    if lo == None:
      shard.index = True
    return shard
  
  @staticmethod
  def get_name(key_name):
    """Returns the name of the RecordIO.

    :param key_name: A datastore key name
    :return: String
    """
    return urllib.unquote(key_name.split(SPLIT_CHAR, 1)[0])
  
  def name(self):
    """Returns the name of the RecordIO this shard belongs to.

    :return: String
    """
    return self.get_name(self.key().name())
  
  def init(self):
    """Initializes internal values."""
    if not hasattr(self, "records_"):
      if self.compressed:
        self.records_ = RecordIORecordsZipped(self.data)
      else:
        self.records_ = RecordIORecords(self.data)
      self.loHi_ = RecordIOShard.lo_hi_from_key(self.key().name())
  
  def commit(self):
    """Writes the data to datastore."""
    self.init()
    self.data = self.records_.get_data(max_size=MAX_BLOB_SIZE)
    if len(self.data) >= MAX_BLOB_SIZE:
      raise RecordIOShardTooBigError()
    self.put()
    
  def not_deleted(self):
    """Entries that need to be deleted in another shard.

    :return: list of keys
    """
    return self.records_.not_deleted()
  
  def __len__(self):
    """The amount of records in this RecordIO. Expensive if compressed.

    :return: int
    """
    self.init()
    return len(self.records_)
  
  def __getitem__(self, key):
    """Returns an the value of an item.

    :param key_entry: An entry tuple (no value needed)
    :return: Object
    """
    return self.records_[key][-1]
  
  def __iter__(self):
    """Yields all entry tuples.

    :return: Entry tuples
    """
    self.init()
    for entry in self.records_:
      yield entry
  
  def __contains__(self, x):
    """Checks whether an entry tuple key is part of this RecordIOShard.

    :param x:  An entry tuple (no value needed)
    :return: Boolean
    """
    try:
      self[x]
      return True
    except:
      return False
  
  def insert(self, entry):
    """Inserts an entry tuple into the RecordIOShard.

    :param entry: An entry tuple
    """
    self.init()
    assert(self.records_.in_range(entry, self.loHi_[0], self.loHi_[1]))
    self.records_.insert(entry)

  def read(self, start_key, end_key):
    """Reads through the records from start_key to end_key (exclusive)

    :param start_key: An entry tuple (no value needed)
    :param end_key: An entry tuple (no value needed)
    :return: Yields all entry tuples within the range.
    """
    self.init()
    for entry in self.records_.read(start_key, end_key):
      yield entry
      
  @staticmethod
  def iterate_records_(records):
    """Iterates over all records.

    :param records: A generator
    :return: A generator
    """
    for x in records:
      yield x

  @staticmethod
  def get_all_query(name, keys_only):
    """Returns a datastore query that returns all shards of a RecordIO.

    :param name: Name of the RecordIO
    :param keys_only: If this should be a keys only query
    :return: A datastore query.
    """
    key_before = db.Key.from_path("RecordIOShard",
                                  urllib.quote(name) +
                                  SPLIT_CHAR + "0" + SPLIT_CHAR)
    key_after = db.Key.from_path("RecordIOShard",
                                 urllib.quote(name) +
                                 SPLIT_CHAR + "0" + SPLIT_CHAR +
                                 SPLIT_CHAR_AFTER +
                                 SPLIT_CHAR_AFTER)
    return RecordIOShard.all(keys_only=keys_only
        ).filter("__key__ >=", key_before).filter("__key__ <", key_after)

  @staticmethod
  def get_shards_for_key_values(name, records, keys_only=True):
    """Given a list of entries, returns the shards where they belong to

    :param name: The name of the RecordIO
    :param records: A list of entry tuples.
    :param keys_only: If only the keys should be returned.
    :return: A list of names or shards.
    """
    gen = RecordIOShard.iterate_records_(records)
    entry = None
    while True:
      if entry == None:
        try:
          entry = gen.next()
        except StopIteration:
          return
      key_before_name = RecordIOShard.key_name(name, hi=entry)
      key_before_name = key_before_name.split(SPLIT_CHAR)
      key_before_name[6] = SPLIT_CHAR_AFTER
      key_before_name = SPLIT_CHAR.join(key_before_name)
      if entry[0] == "":
        key_before_name = (key_before_name.split(SPLIT_CHAR)[0] +
                           SPLIT_CHAR + "0" + SPLIT_CHAR)
      key_before = db.Key.from_path(
          "RecordIOShard",
          key_before_name)
      shard_obj = RecordIOShard.get_all_query(name, keys_only=keys_only).filter(
          "__key__ >", key_before).get()
      if shard_obj == None:
        yield None, [entry] + list(gen)
        return
      shard_key = None
      key_result = shard_obj
      if keys_only:
        shard_key = shard_obj.name()
        key_result = shard_key
      else:
        shard_key = shard_obj.key().name()
      lo, hi = RecordIOShard.lo_hi_from_key(shard_key)
      result = []
      try:
        while entry and not RecordIORecords.in_range(entry, lo, hi):
          result.append(entry)
          entry = gen.next()
      except StopIteration:
        entry = None
        
      if result:
        yield None, result
      result = []
      try:
        while entry and RecordIORecords.in_range(entry, lo, hi):
          result.append(entry)
          entry = gen.next()
      except StopIteration:
        entry = None
      if result:
        yield key_result, result

  def split(self):
    """Splits a RecordIOShard into two smaller shards.

    :return: lo_shard, hi_shard
    """
    self.init()
    name = self.name()
    original_lo, original_hi = self.lo_hi()
    lo_data, hi_data, middle = self.records_.split()
    middle_key = middle[0:4]
    if len(middle_key) == 2:
      middle_key = middle[0:1]
    lo_shard = RecordIOShard.create(name, original_lo, middle_key)
    lo_shard.data = lo_data
    lo_shard.compressed = self.compressed
    hi_shard = RecordIOShard.create(name, middle_key, original_hi)
    hi_shard.data = hi_data
    hi_shard.compressed = self.compressed
    return lo_shard, hi_shard
  
  @staticmethod
  def lo_hi_from_key(key_name):
    """Given a datastore keyname, returns the lo, hi entry tuples.

    :param key_name: String
    :return: (lo, hi) entry tuples
    """
    lo = key_name.split(SPLIT_CHAR)[6:10]
    if lo[0]:
      lo = [binascii.unhexlify(lo[0])] + [int(x) for x in lo[1:]]
      lo = tuple(lo)
    else:
      lo = None
    hi = key_name.split(SPLIT_CHAR)[2:6]
    if hi[0] != SPLIT_CHAR_AFTER:
      hi = [binascii.unhexlify(hi[0])] + [int(x) for x in hi[1:]]
      hi = tuple(hi)
    else:
      hi = None
    return lo, hi
  
  def lo_hi(self):
    """Returns the lo, hi entry tuples of a shard.

    :return:(lo, hi) entry tuples
    """
    self.init()
    return self.loHi_

class RecordIOShardTooBigError(Exception):
  """Gets raised when a shard is too big for datastore"""
  pass

class RecordIOShardDoesNotExistError(Exception):
  """Gets raised if a shard temporarily doesn't exist because it gets split"""
  def __init__(self, name):
    self.name = name
  
  def __str__(self):
    return "Shard does not exist: %s" % (self.name) 
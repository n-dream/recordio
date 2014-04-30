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


from recordio import recordio_chunks
from recordio.bisect_left import bisect_left
import marshal

MARSHAL_VERSION = 2

class RecordIORecords():
  """A sorted list of non-zipped RecordIO entry tuples.

  They either have length 1,2,4,5:

  Length 1: (key,) a deleted normal entry.
  Length 2: (key, value) a normal entry.
  Length 4: (key, i, total, version) a deleted big entry.
  Length 5: (key, i, total, version, value) a big entry.

  Definitions for entries:
  - key: The key of the item
  - i: The i-th chunk of the entry (for big entries)
  - total: The total amount of chunks of this entry (for big entries)
  - version: The version (some hash) of this this entry (for big entries)
  - value: The value of this entry (normal) or of this chunk (for big entries)


  Entries are either of length 2 (key, value) or
  of length 5 (key, shard, total_shards, version, value).
  """
  def __init__(self, data=None):
    if data:
      # The actual list
      self.records_ = marshal.loads(data)
    else:
      self.records_ = []
    # Keys that need to be deleted in another shard.
    self.not_deleted_ = set([])
  
  def get_data(self, max_size=None):
    """Returns the data of this RecordIORecords as a string.

    :param max_size: Ignored.
    :return: String
    """
    return marshal.dumps([x for x in self.records_
                          if not self.is_entry_deleted(x)])
  
  def not_deleted(self):
    """Entries that need to be deleted in another shard.

    :return: list of keys
    """
    result = RecordIORecords()
    for x in self.not_deleted_:
      result.insert(x)
    return result

  def __len__(self):
    """The amount of entries in this RecordIORecords.

    Includes deleted ones, that weren't removed yet.

    :return: int
    """
    return len(self.records_)
  
  def __getitem__(self, key_entry):
    """Returns an item.

    :param key_entry: An entry tuple (no value needed)
    :return: An entry tuple
    """
    try:
      entry = self.read(key_entry).next()
      if self.entry_comperator(entry, key_entry) == 0:
        return entry
    except:
      pass
    raise KeyError(key_entry)
  
  def __iter__(self):
    """Yields all entry tuples.

    :return: Entry tuples
    """
    for entry in self.records_:
      yield entry
  
  def __contains__(self, x):
    """Checks whether an entry tuple key is part of this RecordIORecords.

    :param x:  An entry tuple (no value needed)
    :return: Boolean
    """
    try:
      self[x]
      return True
    except:
      return False
  
  @staticmethod
  def is_entry_deleted(entry):
    """Checks whether an entry is marked as deleted.

    :param entry:  An entry tuple (no value needed)
    :return: Boolean
    """
    return len(entry) == 1 or len(entry) == 4
  
  def insert(self, entry):
    """Inserts an entry tuple into the RecordIORecords.

    :param entry: An entry tuple
    :return: Returns True if the key existed before.
    """
    pos = bisect_left(self.records_, entry, comperator=self.entry_comperator)
    if (pos < len(self.records_) and
        self.entry_comperator(self.records_[pos], entry) == 0):
      existing_entry = self.records_[pos]
      if len(existing_entry) == 5 and existing_entry[1] == 0:
        del_key = existing_entry[0]
        del_amount = existing_entry[2]
        del_hash = existing_entry[3]
        for i in xrange(1, del_amount):
          # POSSIBLE OPTIMIZATION: Do not do binary searches
          del_entry = (del_key, i, del_amount, del_hash)
          if not self.insert(del_entry):
            self.not_deleted_.add(del_entry)
      self.records_[pos] = entry
      return True
    else:
      self.records_.insert(pos, entry)
      return not self.is_entry_deleted(entry)
  
  def read(self, start_key=("",), end_key=None):
    """Reads through the records from start_key to end_key (exclusive)

    :param start_key: An entry tuple (no value needed)
    :param end_key: An entry tuple (no value needed)
    :return: Yields all entry tuples within the range.
    """
    pos = bisect_left(self.records_, start_key,
                      comperator=self.entry_comperator)
    for entry in self.records_[pos:]:
      if end_key != None and self.entry_comperator(entry, end_key) >= 0:
        break
      yield entry
  
  def split(self):
    """Split a RecordIORecords data into two even chunks.

    :return: lower_entries, higher_entries, middle_entry
    """
    if len(self) <= 1:
      raise RecordIOTooSmallToSplitError()
    lo_records = []
    hi_records = []
    lo_size = 0
    hi_size = 0
    left = -1
    right = len(self)
    while left + 1 != right:
      if lo_size <= hi_size:
        left += 1
        lo_records.append(self.records_[left])
        lo_size += recordio_chunks.size(self.records_[left])
      else:
        right -= 1
        hi_records.insert(0,self.records_[right])
        hi_size += recordio_chunks.size(self.records_[right])
    middle_entry = hi_records[0]
    return (marshal.dumps(lo_records, MARSHAL_VERSION),
            marshal.dumps(hi_records, MARSHAL_VERSION),
            middle_entry)
  
  @staticmethod
  def in_range(entry, lo=None, hi=None):
    """Checks whether an entry is belongs to certain range.

    :param entry: An entry tuple (no value needed)
    :param lo: An entry tuple (no value needed)
    :param hi: An entry tuple (no value needed) Exclusive.
    :return: Boolean
    """
    if lo != None and RecordIORecords.entry_comperator(entry, lo) == -1:
      return False
    if hi != None and RecordIORecords.entry_comperator(entry, hi) >= 0:
      return False
    return True
  
  @staticmethod
  def entry_comperator(a, b):
    """Compares two entries.

    :param a: An entry tuple (no value needed)
    :param b: An entry tuple (no value needed)
    :return: Boolean
    """
    if a[0] < b[0]:
      return -1
    elif a[0] > b[0]:
      return 1
    elif len(a) <= 2 and len(b) <= 2:
      return 0
    if len(a) <= 2:
      a = (a[0], 0, 1, 0)
    if len(b) <= 2:
      b = (b[0], 0, 1, 0)
    if a[1] == 0 and b[1] == 0:
      return 0
    elif a[0:4] < b[0:4]:
      return -1
    elif a[0:4] > b[0:4]:
      return 1
    else:
      return 0

class RecordIOTooSmallToSplitError(Exception):
  """Gets raised if a RecordIORecords is too small to be split."""
  pass
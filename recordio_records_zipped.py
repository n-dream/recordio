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
from recordio.recordio_records import RecordIORecords, MARSHAL_VERSION, RecordIOTooSmallToSplitError
from recordio.bisect_left import bisect_left

import marshal
import zlib

COMPRESSION_LEVEL = 1
COMPRESSION_LEVEL_MIN = 0
ZIP_CHUNKS = 2**18

class RecordIORecordsZipped(RecordIORecords):
  """A sorted list of zipped RecordIO entry tuples.

  See RecordIORecords.

  The entry tuples are zipped together into chunks and uncompressed on demand.
  zipped_chunks_ is a list of (lo_entry, hi_entry, zipped_entries).
  lo_entry and hi_entry ARE INCLUSIVE!
  Additionally there is a overwrite list stored in self.records_
  """
  def __init__(self, data=None):
    if data:
      # A list of zipped entry tuples. See Class definition.
      self.zipped_chunks_ = marshal.loads(data)
    else:
      self.zipped_chunks_ = []
    # The overwrite list.
    self.records_ = []
    # Keys that need to be deleted in another shard.
    self.not_deleted_ = set([])
  
  def get_data(self, max_size=None, force_repackiging=False):
    """Returns the data of this RecordIORecords as a string.

    :param max_size: The max size of the returned data.
    :param force_repackiging: If the the chunks should be unzipped and rezipped
                              to be more even.
    :return: String
    """
    self.zipped_chunks_ = list(self.get_zipped_chunks_(force_repackiging))
    self.records_ = []
    result = marshal.dumps(self.zipped_chunks_, MARSHAL_VERSION)
    if not force_repackiging and max_size != None and len(result) >= max_size:
      return self.get_data(max_size, True)
    return result
  
  def insert(self, entry):
    """Inserts an entry tuple into the RecordIORecords.

    :param entry:  An entry tuple
    """
    pos = bisect_left(self.zipped_chunks_, (entry, entry),
                      comperator=self.zip_chunk_comperator)
    if ((pos < len(self.zipped_chunks_) and
        self.zip_chunk_comperator(self.zipped_chunks_[pos], (entry, entry)) == 0)
        or self.is_entry_deleted(entry)):
      RecordIORecords.insert(self, entry)
    else:
      self.zipped_chunks_.insert(pos,
          (entry[:-1], entry[:-1],
           zlib.compress(marshal.dumps([entry], MARSHAL_VERSION),
                             COMPRESSION_LEVEL_MIN)))

  def __len__(self):
    """The amount of entries in this RecordIORecords.

    Expensive because everything needs to be unzipped!
    Includes deleted ones, that weren't removed yet.

    :return: int
    """
    length = 0
    for x in self:
      length += 1
    return length

  def read(self, start_key=("",), end_key=None):
    """Reads through the records from start_key to end_key (exclusive)

    :param start_key: An entry tuple (no value needed)
    :param end_key: An entry tuple (no value needed)
    :return: Yields all entry tuples within the range.
    """
    # POSSIBLE OPTIMIZATION: Do binary search over zipped records to find start
    for entry in self:
      if end_key != None and self.entry_comperator(entry, end_key) >= 0:
        break
      if self.entry_comperator(entry, start_key) >= 0:
        yield entry

  def entries_from_records_(self):
    """Yields all entry tuples from the overwrite list.

    :return: entry tuples.
    """
    for x in self.records_:
      yield x

  def entries_from_zipped_chunks_(self):
    """Yields all entry tuples from the zipped chunks.

    :return: entry tuples.
    """
    for chunk in self.zipped_chunks_:
      records = marshal.loads(zlib.decompress(chunk[2]))
      for x in records:
        yield x

  def get_next_entry_(self, gen):
    """Returns the next entry of a generator or None.

    :param gen: A generator
    :return: The next item or None if there is none.
    """
    try:
      return gen.next()
    except:
      pass

  def __iter__(self):
    """Yields all entry tuples.

    :return: Entry tuples
    """
    records = self.entries_from_records_()
    zipped_chunks = self.entries_from_zipped_chunks_()
    records_value = self.get_next_entry_(records)
    zipped_chunks_value = self.get_next_entry_(zipped_chunks)
    while records_value != None and zipped_chunks_value != None:
      comparison = self.entry_comperator(records_value, zipped_chunks_value)
      if comparison <= 0:
        if not self.is_entry_deleted(records_value):
          yield records_value
        records_value = self.get_next_entry_(records)
        if comparison == 0:
          if (len(zipped_chunks_value) == 5 and
              zipped_chunks_value[1] == 0):
            del_key = zipped_chunks_value[0]
            del_amount = zipped_chunks_value[2]
            del_hash = zipped_chunks_value[3]
            for i in xrange(1, del_amount):
              self.not_deleted_.add((del_key, i,
                                  del_amount, del_hash))
          zipped_chunks_value = self.get_next_entry_(zipped_chunks)
      else:
        if zipped_chunks_value[:4] in self.not_deleted_:
          self.not_deleted_.remove(zipped_chunks_value[:4])
        else:
          yield zipped_chunks_value
        zipped_chunks_value = self.get_next_entry_(zipped_chunks)
    while records_value:
      if not self.is_entry_deleted(records_value):
        yield records_value
      records_value = self.get_next_entry_(records)
    while zipped_chunks_value:
      if zipped_chunks_value[:4] in self.not_deleted_:
        self.not_deleted_.remove(zipped_chunks_value[:4])
      else:
        yield zipped_chunks_value
      zipped_chunks_value = self.get_next_entry_(zipped_chunks)


  def get_zipped_chunks_(self, force_repackiging=False):
    """Returns all zipped chunks for this RecordIORecordsZipped.


    :param force_repackiging: If the zip chunks should be unzipped and rezipped
                              to be more even.
    :return: list of zipped chunks: (lo_entry, hi_entry, zipped_entries).
             lo_entry and hi_entry ARE INCLUSIVE!
    """
    if self.records_ or force_repackiging:
      for records in recordio_chunks.get_chunks(self, ZIP_CHUNKS):
        yield (records[0][:-1], records[-1][:-1],
               zlib.compress(marshal.dumps(records, MARSHAL_VERSION),
                             COMPRESSION_LEVEL))
    else:
      for x in self.zipped_chunks_:
        yield x


  def split(self):
    """Split a RecordIORecordsZipped data into two even chunks.

    :return: lower_entries, higher_entries, middle_entry
    """
    new_zipped_chunks = list(self.get_zipped_chunks_())
    if len(new_zipped_chunks) <= 1:
      raise RecordIOTooSmallToSplitError()
    lo_chunks = []
    hi_chunks = []
    lo_size = 0
    hi_size = 0
    left = -1
    right = len(new_zipped_chunks)
    while left + 1 != right:
      if lo_size <= hi_size:
        left += 1
        lo_chunks.append(new_zipped_chunks[left])
        lo_size += len(new_zipped_chunks[left][2])
      else:
        right -= 1
        hi_chunks.insert(0, new_zipped_chunks[right])
        hi_size += len(new_zipped_chunks[right][2])
    middle_entry_lo = new_zipped_chunks[right][0]
    self.records_ = []
    self.zipped_chunks_ = lo_chunks + hi_chunks
    return (marshal.dumps(lo_chunks, MARSHAL_VERSION),
            marshal.dumps(hi_chunks, MARSHAL_VERSION),
            middle_entry_lo)

  @staticmethod
  def zip_chunk_comperator(a, b):
    """Compares two zipped chunks.

    :param a: zipped chunk tuple (See class definition).
    :param b: zipped chunk tuple (See class definition).
    :return: Boolean
    """
    a_lo, a_hi = a[:2]
    b_lo, b_hi = b[:2]
    if RecordIORecords.entry_comperator(a_hi, b_lo) == -1:
      return -1
    elif RecordIORecords.entry_comperator(a_lo, b_hi) == 1:
      return 1
    return 0
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

from recordio.recordio_shard import RecordIOShard, RecordIOShardTooBigError, RecordIOShardDoesNotExistError, INTEGER_MAX, MAX_BLOB_SIZE, SPLIT_CHAR
from recordio.recordio_chunks import get_chunks
from recordio.recordio_records import RecordIORecords, MARSHAL_VERSION
from recordio import recordio_entry_types
from google.appengine.runtime.apiproxy_errors import RequestTooLargeError, ArgumentError
from google.appengine.api.datastore_errors import BadRequestError
from google.appengine.api import taskqueue
from google.appengine.ext import db

import cPickle
import time
import marshal
import math
import logging
import datetime
import urllib

class RecordIOWriter():
  """This class allows you to write data to a RecordIO."""
  def __init__(self, name):
    """Creates a RecordIOWriter

    :param name: The name of the RecordIO. The urllib quoted name is not
                 allowed to be longer than 64 characters.
    """
    if len(urllib.quote(name)) > MAX_KEY_LENGTH:
      raise ValueError("Max urllib.quote(name) length is %d: len('%s') is %d" %
                       (MAX_KEY_LENGTH, name, len(urllib.quote(name))))
    self.name = name
    self.updates = RecordIORecords()
    self.pending_worker_tasks = []
    self.db_search = 0
    self.db_get = 0
    self.db_put = 0
  
  def create(self, compressed=True, pre_split=[]):
    """Creates a RecordIO in datastore. If the RecordIO exists, nothing happens

    :param compressed: Boolean if the data in the RecordIO should be gzipped.
    :param pre_split: An optional list of keys to that should be used to
                      pre-split the internal data shards. This is only makes
                      sense if you are going to write a lot of data and you
                      already know the key range of the data and roughly how
                      many entries fit into one shard.
    :return: True, if the RecordIO didn't exist before.
    """
    self.db_search += 1
    if RecordIOShard.get_all_query(self.name, keys_only=True).get() == None:
      pre_split.sort()
      self.db_put += 1
      split = [None] + [(x,) for x in pre_split] + [None]
      split = [(split[i], split[i+1]) for i in xrange(len(split) - 1)]
      for lo, hi in split:
        index = None
        if lo == None:
          index = True
        RecordIOShard.get_or_insert(RecordIOShard.key_name(self.name,
                                                          lo=lo, hi=hi),
                                    compressed=compressed, index=index)
      return True
    return False
  
  def delete(self):
    """Deletes a RecordIO.

    Modifying RecordIOs or applying queued writes may result in errors during
    deletions.
    """
    db.delete(RecordIOShard.get_all_query(self.name, keys_only=True))
  
  def insert(self, key, value):
    """Assigns a value to a given key.

    Overwrites existing values with the same key.

    :param key: Must be a string and must not be longer than 64 characters.
    :param value:  Values can be of any type that is pickeable (anything you
                   can put in memcache). Values can have arbitrary size
                   (There is no size limit like normal Datastore entries have).
    """
    if isinstance(key, unicode):
      try:
        key = str(key)
      except:
        pass
    if not isinstance(key, str):
      raise ValueError("Key must be <type 'str'> got: %s" % type(key))
    typed_value = None
    if isinstance(value, str):
      typed_value = recordio_entry_types.STRING + value
    elif type(value) in MARSHALABLE_TYPES:
      try:
        typed_value = recordio_entry_types.MARSHAL + marshal.dumps(
                          value, MARSHAL_VERSION)
      except:
        pass
    if typed_value == None:
      typed_value = recordio_entry_types.CPICKLE + cPickle.dumps(value)
    if len(key) > MAX_KEY_LENGTH:
      raise ValueError("Max key length is %d: %d" %
                       (MAX_KEY_LENGTH, len(key)))
    if len(typed_value) > MAX_ENTRY_SIZE:
      entries = int(math.ceil(1.0 * len(typed_value) / MAX_ENTRY_SIZE))
      version = (hash(typed_value) + hash(str(time.time()))) % INTEGER_MAX
      for i in xrange(entries):
        self.insert_entry_((key, i, entries, version,
                           typed_value[i * MAX_ENTRY_SIZE:
                                       (i+1) * MAX_ENTRY_SIZE]))
    else:
      self.insert_entry_((key, typed_value))
  
  def remove(self, key):
    """Removes a value from the RecordIO

    :param key: A key of a previously inserted value. If this key does not
                exist, no exception is thrown.
    """
    self.updates.insert((key, ))
  
  def commit_sync(self, retries=32, retry_timeout=1):
    """Applies all changes synchronously to the RecordIO.

    :param retries: How many times a commit_sync should be retried in case of
                    datastore collisions.
    :param retry_timeout: The amount of second to wait before the next retry.
    """
    if not len(self.updates):
      return
    for attempt in range(retries + 1):
      shard_does_not_exist = RecordIORecords()
      for shard_name, key_values in RecordIOShard.get_shards_for_key_values(
          self.name, self.updates):
        self.db_search += 1
        if shard_name == None and key_values:
          logging.debug("RecordIO %s: No shard found for:\n%s -> %s" %
              (self.name, 
               SPLIT_CHAR.join(RecordIOShard.entry_key(key_values[0])),
               key_values[0][:-1]))
          for entry in key_values:
            shard_does_not_exist.insert(entry)
        else:
          lo_just_split = None
          hi_just_split = None
          for key_values_chunk in get_chunks(key_values, MAX_WRITE_BATCH_SIZE):
            if lo_just_split and hi_just_split and key_values_chunk:
              if RecordIORecords.in_range(key_values_chunk[0],
                                          lo=lo_just_split[0],
                                          hi=lo_just_split[1]):
                shard_name = RecordIOShard.key_name(self.name,
                                                   lo=lo_just_split[0],
                                                   hi=lo_just_split[1])
              elif RecordIORecords.in_range(key_values_chunk[0],
                                            lo=hi_just_split[0],
                                            hi=hi_just_split[1]):
                shard_name = RecordIOShard.key_name(self.name,
                                                    lo=hi_just_split[0],
                                                    hi=hi_just_split[1])
            not_deleted = None
            try:
              not_deleted, lo_just_split, hi_just_split = self.commit_shard_(
                  shard_name, key_values_chunk)
            except RecordIOShardDoesNotExistError:
              logging.debug("Shard does not exist:\n" + shard_name)
              lo_just_split = None
              hi_just_split = None
              for entry in key_values_chunk:
                shard_does_not_exist.insert(entry)
            if not_deleted:
              for to_delete_shard_name, to_delete_key_values in (
                   RecordIOShard.get_shards_for_key_values(
                       self.name, not_deleted)):
                self.db_search += 1
                try:
                  self.commit_shard_(to_delete_shard_name, to_delete_key_values)
                except RecordIOShardDoesNotExistError:
                  logging.debug("Shard does not exist:\n" + shard_name)
                  for entry in to_delete_key_values:
                    shard_does_not_exist.insert(entry)
      self.updates = shard_does_not_exist
      if len(self.updates):
        if attempt == retries:
          raise RecordIOWriterNotCompletedError(len(self.updates))
        else:
          logging.debug("Commit attempt %d failed" % attempt)
          time.sleep(retry_timeout)
      else:
        return
  
  def commit_async(self, write_every_n_seconds=300):
    """Applies the changes asynchronously to the RecordIO.

    Automatically batches other pending writes to the same RecordIO (Cheaper
    and more efficient than synchronous commits).

    :param write_every_n_seconds: Applies the changes after this amount of
                                  seconds to the RecordIO.
    """
    seen = set([])
    raise_exception = False
    try:
      for tag in self.commit_to_queue_():
        if tag in seen:
          continue
        seen.add(tag)
        self.pending_worker_tasks.append(
            self.create_task_(tag, write_every_n_seconds))
    except RecordIOWriterNotCompletedError:
      raise_exception = True
    
    failed_add = []
    while self.pending_worker_tasks:
      batch = self.pending_worker_tasks[:100]
      self.pending_worker_tasks = self.pending_worker_tasks[100:]
      try:
        taskqueue.Queue('recordio-writer').add(batch)
      except (taskqueue.DuplicateTaskNameError, taskqueue.TombstonedTaskError,
              taskqueue.TaskAlreadyExistsError):
        pass
      except ValueError:
        failed_add += batch
    self.pending_worker_tasks = failed_add
  
    if raise_exception or self.pending_worker_tasks:
      raise RecordIOWriterNotCompletedError(len(self.updates))

  def db_stats(self):
    """Returns some datastore access statistics.

    :return: Dict
    """
    return { "search": self.db_search, "get": self.db_get, "put": self.db_put }

  def insert_entry_(self, entry):
    """Inserts a entry tuples to the internal queue.

    :param entry: An entry tuple.
    """
    self.updates.insert(entry)

  @staticmethod
  def create_task_(tag, write_every_n_seconds=300, in_past=False):
    """Creates the future taskqueue tasks to apply queued writes.

    :param tag: The shard to write.
    :param write_every_n_seconds: At what interval the shard should be updated.
    :param in_past: If the we should schedule the task in the past
    :return: taskqueue.Task
    """
    now = int(time.time())
    schedule = now - (now % write_every_n_seconds)
    schedule += hash(tag) % write_every_n_seconds
    if schedule < now and not in_past:
      schedule += write_every_n_seconds
    if schedule > now and in_past:
      schedule -= write_every_n_seconds
    task_name = "%d_%d_%d" % (hash(tag[:len(tag)/2]),
                              hash(tag[len(tag)/2:]),
                              schedule)
    
    params = {"taskqueue": tag}
    return taskqueue.Task(name=task_name,
                          url="/recordio/write",
                          params=params,
                          eta=datetime.datetime.fromtimestamp(
                              schedule + MAX_CLOCK_SKEW))

  def commit_to_queue_(self):
    """Adds all pending changes to the task queues for async commits

    :return: Yields all shard names that need to be updated.
    """
    pull = taskqueue.Queue('recordio-queue')
    rpcs = []
    key_values_not_added = RecordIORecords()
    for shard_name, key_values in RecordIOShard.get_shards_for_key_values(
          self.name, self.updates):
      self.db_search += 1
      if shard_name == None:
        for entry in key_values:
          key_values_not_added.insert(entry)
      else:
        for key_values_chunk in get_chunks(key_values, MAX_TASKQUEUE_BATCH_SIZE):
          payload = marshal.dumps(key_values_chunk, MARSHAL_VERSION)
          rpc = pull.add_async(taskqueue.Task(payload=payload, method='PULL',
                                              tag=shard_name))
          rpcs.append((rpc, key_values_chunk, shard_name))
    
    for rpc, key_values, shard_name in rpcs:
      try:
        rpc.get_result()
        yield shard_name
      except:
        for entry in key_values:
          key_values_not_added.insert(entry)
    self.updates = key_values_not_added
    if len(self.updates):
      raise RecordIOWriterNotCompletedError(len(self.updates))

  @db.transactional(xg=True)
  def commit_shard_(self, shard_name, key_values):
    """Adds key, values to a shard and splits it if necessary.

    :param shard_name: The key name of the RecordIOShard.
    :param key_values: A list of key values to be added
    :return: list of keys that need to be deleted in other shards.
    """
    shard = RecordIOShard.get_by_key_name(shard_name)
    self.db_get += 1
    if shard == None:
      raise RecordIOShardDoesNotExistError(shard_name)
    for entry in key_values:
      shard.insert(entry)
    try:
      shard.commit()
      self.db_put += 1
      return (shard.not_deleted(), None, None)
    except (RecordIOShardTooBigError,
            RequestTooLargeError, ValueError, ArgumentError, BadRequestError):
      shard.delete()
      lo_shard, hi_shard = shard.split()
      lo_shard.commit()
      hi_shard.commit()
      self.db_put += 2
      logging.debug("Split\n%s\n%s\n%s" % (shard.key().name(),
                                            lo_shard.key().name(),
                                            hi_shard.key().name()))
      shard_name = hi_shard.key().name()
      return shard.not_deleted(), (lo_shard.lo_hi()), (hi_shard.lo_hi())

class RecordIOWriterNotCompletedError(Exception):
  """Gets raised if the not all changes were applied."""
  def __init__(self, amount):
    self.amount = amount
  
  def __unicode__(self):
    return (u"RecordIOWriter failed to write %s entries" % (self.amount))

# The maximum length of a key for a value.
MAX_KEY_LENGTH = 64
# The maximum size of an entry tuples before it needs to be split.
MAX_ENTRY_SIZE = 2**15
# Types that are marshalable.
MARSHALABLE_TYPES = set([bool, int, long, float, complex, unicode, tuple, list,
                         set, frozenset, dict])
# The maximum size of a write batch.
# leave 10KB for other properties, entries keys and list overhead
MAX_WRITE_BATCH_SIZE = MAX_BLOB_SIZE - 2*MAX_ENTRY_SIZE - 10000
# The maximum size of a taskqueue batch rpc.
MAX_TASKQUEUE_BATCH_SIZE = MAX_WRITE_BATCH_SIZE
# The maximum clock skew that App Engine servers might have.
MAX_CLOCK_SKEW = 30
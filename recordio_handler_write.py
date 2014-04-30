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


from recordio.recordio_writer import RecordIOWriter, RecordIOWriterNotCompletedError, MAX_TASKQUEUE_BATCH_SIZE, MAX_WRITE_BATCH_SIZE, MAX_CLOCK_SKEW
from recordio.recordio_shard import RecordIOShard
from recordio import recordio_chunks

from google.appengine.api import taskqueue

import webapp2
import marshal
import logging

MAX_RPC_SIZE = 32000000
LEASE_TIME_PER_BATCH = 60

class WriteHandler(webapp2.RequestHandler):
  """Applies pending writes from RecordIO taskqueues."""
  def commit_batch(self, tag, batch):
    """Applies a batch of values to a RecordIO and deletes the taskqueue task,

    :param tag: The current tag we are working on
    :param batch: A list of (tasqueue_task, key_value_list)
    :return: True on success
    """
    if batch:
      done_tasks = []
      count = 0
      writer = RecordIOWriter(RecordIOShard.get_name(tag))
      for done_task, key_values in batch:
        done_tasks.append(done_task)
        for entry in key_values:
          writer.insert_entry_(entry)
          count += 1
      try:
        writer.commit_sync(retries=1)
        try:
          self.pull.delete_tasks(done_tasks)
        except taskqueue.BadTaskStateError:
          for task in done_tasks:
            if task.was_deleted:
              continue
            try:
              self.pull.delete_tasks(task)
            except taskqueue.BadTaskStateError:
              logging.debug("RecordIO Failed to free task %s on %s" %
                            task.name, tag)
        logging.debug("RecordIO wrote %d entries to %s" %
                      (count, writer.name))
      except RecordIOWriterNotCompletedError:
        logging.debug("RecordIO not completed on: %s" % tag)
        for task in done_tasks:
          self.pull.modify_task_lease(task, 0)
        return False
    return True
      
  def get(self):
    self.pull = taskqueue.Queue('recordio-queue')
    tag = self.request.get("taskqueue")
    max_tasks_to_lease = MAX_RPC_SIZE / MAX_TASKQUEUE_BATCH_SIZE
    if tag:
      batch = []
      batch_size = 0
      success = True
      while True:
        tasks = self.pull.lease_tasks_by_tag(LEASE_TIME_PER_BATCH,
                                             max_tasks_to_lease, tag=tag)
        for task in tasks:
          if task.was_deleted:
            # Should never happend
            continue
          next_key_values = marshal.loads(task.payload)
          next_size = sum([recordio_chunks.size(x) for x in next_key_values])
          if next_size + batch_size >= MAX_WRITE_BATCH_SIZE:
            success = success and self.commit_batch(tag, batch)
            batch = [(task, next_key_values)]
            batch_size = next_size
          else:
            batch_size += next_size
            batch.append((task, next_key_values))
        if len(tasks) != max_tasks_to_lease:
          break
      success = success and self.commit_batch(tag, batch)
      if not success:
        raise Exception("RecordIO not completed")
    else:
      pending_tasks = self.pull.lease_tasks(0, max_tasks_to_lease)
      seen = set([])
      for task in pending_tasks:
        tag = task.tag
        if tag in seen:
          continue
        seen.add(tag)
        try:
          taskqueue.Queue('recordio-writer').add(
              RecordIOWriter.create_task_(tag, in_past=True))
          self.response.out.write("Scheduled write for: %s<br>" % tag)
        except (taskqueue.DuplicateTaskNameError,
                taskqueue.TombstonedTaskError,
                taskqueue.TaskAlreadyExistsError):
          self.response.out.write("Already pending write for: %s<br>" % tag)
      if len(pending_tasks) == max_tasks_to_lease:
        self.response.out.write(
            "<script type=text/javascript>window.setTimeout(function() {"
            "document.location.reload();"
            "}, 5000);</script>")
      
  
  def post(self):
    self.get()

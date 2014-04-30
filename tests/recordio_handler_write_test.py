from google.appengine.api import taskqueue
from google.appengine.ext import testbed

import base64
import unittest
import urlparse

from recordio.tests import test_helper
from recordio.recordio_writer import RecordIOWriter, MAX_ENTRY_SIZE
from recordio.recordio_handler_write import WriteHandler
from recordio.recordio_reader import RecordIOReader
from recordio.recordio_shard import RecordIOShard, MAX_BLOB_SIZE
from recordio.recordio_entry_types import STRING

class RecordIOHandlerTestCase(unittest.TestCase):
  def setUp(self):
    test_helper.setUp(self)
  
  def tearDown(self):
    test_helper.tearDown(self)

  def testTaskQueue(self):
    writer = RecordIOWriter("test")
    writer.create(compressed=False)
    test_value = test_helper.uncompressableString(MAX_ENTRY_SIZE-1)
    entries_to_write = MAX_BLOB_SIZE / MAX_ENTRY_SIZE + 1
    for i in range(entries_to_write):
      writer.insert(str(i), test_value)
    writer.commit_async()
    
    taskq = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
    tasks = taskq.GetTasks("recordio-writer")
    for task in tasks:
      url=task["url"]
      args = urlparse.parse_qs(base64.b64decode(task["body"]))
      for x in args:
        args[x] = args[x][0]
      test_helper.requestGet(WriteHandler(), url, args)
    assert(len([x for x in RecordIOShard.all()]) > 1)
    reader = RecordIOReader("test")
    result = {}
    for key, value in reader:
      result[key] = value
    self.assertEqual(len(result), entries_to_write)
    for i in range(entries_to_write):
      self.assertEqual(result[str(i)], test_value, "Not equal")
    
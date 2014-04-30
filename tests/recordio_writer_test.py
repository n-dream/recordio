from google.appengine.api import taskqueue
from google.appengine.ext import testbed

import unittest
import urllib
import marshal
import cPickle
import base64

from recordio.recordio_shard import RecordIOShard, RecordIOShardDoesNotExistError
from recordio.recordio_entry_types import STRING, MARSHAL, CPICKLE
from recordio.recordio_writer import RecordIOWriter, MAX_ENTRY_SIZE, MAX_TASKQUEUE_BATCH_SIZE, RecordIOWriterNotCompletedError
from recordio.tests import test_helper

class RecordIOWriterTestCase(unittest.TestCase):
  def setUp(self):
    test_helper.setUp(self)
  
  def tearDown(self):
    test_helper.tearDown(self)

  def writeOneShard(self, compressed):
    updater = RecordIOWriter("test")
    updater.create(compressed=compressed)
    updater.insert("1", "foo")
    updater.insert("2", "bar")
    updater.commit_sync()
    updater = RecordIOWriter("test")
    updater.insert("3", "win")
    updater.remove("2")
    updater.commit_sync()
    recordio = RecordIOShard.all().get()
    self.assertEqual(recordio.compressed, compressed)
    self.assertEqual([x for x in recordio], [("1", STRING + "foo"), ("3", STRING + "win")])

  def testWriteOneShardCompressed(self):
    self.writeOneShard(True)

  def testWriteOneShardUncompressed(self):
    self.writeOneShard(False)

  def testWriteStringMarshalPickle(self):
    updater = RecordIOWriter("test")
    updater.create()
    updater.insert("string", "string")
    marshalable = {"a": [1,2,3]}
    updater.insert("marshal", marshalable)
    class AnyClass():
      pass
    pickleable = AnyClass()
    updater.insert("cpickle", pickleable)
    updater.commit_sync()
    recordio = RecordIOShard.all().get()
    self.assertEqual([x for x in recordio],
                     [("cpickle", CPICKLE + cPickle.dumps(pickleable)),
                      ("marshal", MARSHAL + marshal.dumps(marshalable)),
                      ("string", STRING + "string")])

  def write2MBAndReplace(self, compressed):
    test_string = test_helper.uncompressableString(2**21)
    updater = RecordIOWriter("test")
    updater.create(compressed=compressed)
    updater.insert("test", test_string)
    updater.commit_sync()
    output = []
    entries = 0
    shards_count = 0
    for recordio in RecordIOShard.all():
      self.assertTrue(len(recordio.data) >= 1000)
      shards_count += 1
      for entry in recordio:
        output += [entry[-1]]
        entries += 1
    self.assertTrue(shards_count > 1)
    self.assertTrue(entries > 3)
    self.assertEqual("".join(output), STRING + test_string, "read != write")
    updater.insert("test", "short")
    updater.commit_sync(retries=0)
    replaced_shards_count = 0
    for recordio in RecordIOShard.all():
      if replaced_shards_count == 0:
        self.assertEqual(1, len(recordio))
        for entry in recordio:
          self.assertEqual(STRING + "short", entry[-1])
      else:
        self.assertEqual(0, len(recordio))
        for entry in recordio:
          self.fail("shouldnt be iterable")
      replaced_shards_count += 1
      self.assertTrue(len(recordio.data) < 1000)
    self.assertTrue(replaced_shards_count > 0)
    self.assertTrue(replaced_shards_count <= shards_count)

  def testWrite2MBUncompressedAndReplace(self):
    self.write2MBAndReplace(False)
  
  def testWrite2MBCompressedAndReplace(self):
    self.write2MBAndReplace(True)

  def testWriteDuringSplit(self):
    recordio = RecordIOShard.create("test", compressed=False)
    recordio.insert(("1", STRING + "1"))
    recordio.insert(("2", STRING + "2"))
    lo_shard, hi_shard = recordio.split()
    lo_shard.commit()
    updater = RecordIOWriter("test")
    updater.insert("3", "3")
    self.assertRaises(RecordIOShardDoesNotExistError,
                      updater.commit_shard_,
                      hi_shard.key().name(), updater.updates)
    self.assertRaises(RecordIOWriterNotCompletedError,
                      updater.commit_sync,
                      32, 0)
    hi_shard.commit()
    updater.insert("0", STRING + "0")
    updater.commit_sync()
    lo_shard, hi_shard = [x for x in RecordIOShard.all()]
    self.assertEqual([x[0] for x in lo_shard], ["0", "1"])
    self.assertEqual([x[0] for x in hi_shard], ["2", "3"])

  def testCommitToQueue(self):
    updater = RecordIOWriter("test")
    updater.create()
    chunk_size = MAX_ENTRY_SIZE - 1
    entries_to_write = MAX_TASKQUEUE_BATCH_SIZE / MAX_ENTRY_SIZE + 1
    for i in xrange(entries_to_write):
      updater.insert(str("%09d" % i),
                     test_helper.uncompressableString(chunk_size))
    list(updater.commit_to_queue_())
    pull = taskqueue.Queue('recordio-queue')
    tasks = list(pull.lease_tasks(60, 100))
    self.assertEqual(len(tasks), 2)
    self.assertEqual(tasks[0].tag, RecordIOShard.key_name("test"))
    self.assertEqual(tasks[1].tag, RecordIOShard.key_name("test"))
    updates_0 = marshal.loads(tasks[0].payload)
    updates_1 = marshal.loads(tasks[1].payload)
    self.assertEqual([str("%09d" % x) for x in xrange(entries_to_write)],
                     [x[0] for x in updates_0] + [x[0] for x in updates_1])
    self.assertTrue(updates_0[0][1] ==
                    STRING + test_helper.uncompressableString(chunk_size))

  def testCommitToQueueSplitEntries(self):
    chunk_size = MAX_ENTRY_SIZE + 1
    test_string = test_helper.uncompressableString(chunk_size)
    updater = RecordIOWriter("test")
    updater.create()
    updater.insert("test", test_string)
    list(updater.commit_to_queue_())
    pull = taskqueue.Queue('recordio-queue')
    tasks = list(pull.lease_tasks(60, 100))
    self.assertEqual(len(tasks), 1)
    self.assertEqual(tasks[0].tag, RecordIOShard.key_name("test"))
    updates = marshal.loads(tasks[0].payload)
    self.assertEqual([('test', 0, 2), ('test', 1, 2)],
                     [x[:-2] for x in updates])
    self.assertEqual(STRING + test_string, "".join([x[-1] for x in updates]))
    
      
  def testCommitToQueueAndScheduleWrite(self):
    updater = RecordIOWriter("test")
    updater.create()
    updater.insert("a", "")
    updater.commit_async()
    taskq = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
    
    tasks = taskq.GetTasks("recordio-writer")
    self.assertEqual(len(tasks), 1)
    self.assertEqual(tasks[0]["url"], "/recordio/write")
    self.assertEqual(base64.b64decode(tasks[0]["body"]),
                     "taskqueue=" + urllib.quote(
                     RecordIOShard.key_name("test")))
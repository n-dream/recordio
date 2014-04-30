from google.appengine.api import taskqueue

import unittest
import marshal
import cPickle

from recordio.recordio_shard import RecordIOShard, RecordIOShardDoesNotExistError
from recordio.recordio_reader import RecordIOReader
from recordio.recordio_entry_types import STRING, MARSHAL, CPICKLE
from recordio.tests import test_helper

class AnyClass():
  def __eq__(self, other):
    return True

class RecordIOReaderTestCase(unittest.TestCase):
  def setUp(self):
    test_helper.setUp(self)
  
  def tearDown(self):
    test_helper.tearDown(self)

  def readFromnOneShard(self, compressed):
    recordio = RecordIOShard.create("test", compressed=compressed)
    recordio.insert(("0", STRING + "a"))
    recordio.insert(("1", STRING + "b"))
    recordio.insert(("2", STRING + "c"))
    recordio.commit()
    reader = RecordIOReader("test")
    self.assertEqual([("0", "a"), ("1", "b"), ("2", "c")], list(reader))
    self.assertEqual([("0", "a"), ("1", "b"), ("2", "c")],
                     list(reader.read(start_key="0")))
    self.assertEqual([("0", "a"), ("1", "b"), ("2", "c")],
                     list(reader.read(end_key="3")))
    self.assertEqual([("1", "b"), ("2", "c")],
                     list(reader.read(start_key="1")))
    self.assertEqual([("0", "a"), ("1", "b")],
                     list(reader.read(end_key="2")))
    self.assertEqual([("1", "b")],
                     list(reader.read(start_key="1", end_key="2")))
    self.assertTrue("0" in reader)
    self.assertFalse("3" in reader)
    self.assertEqual(reader["0"], "a")

  def testReadFromOneShardCompressed(self):
    self.readFromnOneShard(True)
  
  def testReadFromOneShardUncompressed(self):
    self.readFromnOneShard(False)
  
  def testReadStringMarshalPickle(self):
    recordio = RecordIOShard.create("test")
    recordio.insert(("string", STRING + "string"))
    marshalable = {"a": [1,2,3, u"asd"]}
    recordio.insert(("marshal", MARSHAL + marshal.dumps(marshalable)))
    pickleable = AnyClass()
    recordio.insert(("cpickle", CPICKLE + cPickle.dumps(pickleable)))
    recordio.commit()
    reader = RecordIOReader("test")
    self.assertEqual([("cpickle", pickleable),
                      ("marshal", marshalable),
                      ("string", "string")], list(reader))
  
  def testReadFromThreeShards(self):
    recordio = RecordIOShard.create("test", hi=("1",))
    recordio.insert(("0", STRING + "a"))
    recordio.commit()
    recordio = RecordIOShard.create("test", lo=("1",), hi=("3",))
    recordio.insert(("1", STRING + "b"))
    recordio.insert(("2", STRING + "c"))
    recordio.commit()
    recordio = RecordIOShard.create("test", lo=("3",))
    recordio.insert(("3", STRING + "d"))
    recordio.commit()
    reader = RecordIOReader("test")
    self.assertEqual([("0", "a"), ("1", "b"), ("2", "c"), ("3", "d")],
                     list(reader))
    self.assertEqual([("0", "a"), ("1", "b"), ("2", "c"), ("3", "d")],
                     list(reader.read(start_key="0")))
    self.assertEqual([("0", "a"), ("1", "b"), ("2", "c"), ("3", "d")],
                     list(reader.read(end_key="4")))
    self.assertEqual([("1", "b"), ("2", "c"), ("3", "d")],
                     list(reader.read(start_key="1")))
    self.assertEqual([("2", "c"), ("3", "d")],
                     list(reader.read(start_key="2")))
    self.assertEqual([("0", "a"), ("1", "b")],
                     list(reader.read(end_key="2")))
    self.assertEqual([("1", "b"), ("2", "c")],
                     list(reader.read(start_key="1", end_key="3")))
    self.assertEqual([("1", "b")],
                     list(reader.read(start_key="1", end_key="2")))
  
    
  def testReadSplitEntries(self):
    recordio = RecordIOShard.create("test", compressed=False)
    recordio.insert(("a", STRING + "a"))
    recordio.insert(("b", 0, 1, 1, STRING + "b"))
    recordio.insert(("c", STRING + "c"))
    recordio.insert(("d", 0, 2, 1, STRING + "1"))
    recordio.insert(("d", 1, 2, 1, "2"))
    recordio.insert(("e", 0, 3, 1, STRING + "1"))
    recordio.insert(("e", 1, 3, 1, "2"))
    recordio.insert(("e", 2, 3, 1, "3"))
    recordio.insert(("f", STRING + "f"))
    recordio.insert(("g", 0, 2, 2, STRING + "1"))
    recordio.insert(("g", 1, 2, 1, "bad"))
    recordio.insert(("g", 1, 2, 2, "2"))
    recordio.insert(("g_missing_1", 0, 3, 1, STRING + "bad"))
    recordio.insert(("g_missing_1", 1, 3, 1, "bad"))
    recordio.insert(("g_missing_2", 1, 2, 1, "bad"))
    recordio.insert(("h", STRING + "h"))
    recordio.commit()
    reader = RecordIOReader("test")
    self.assertEqual([("a", "a"),
                      ("b", "b"),
                      ("c", "c"),
                      ("d", "12"),
                      ("e", "123"),
                      ("f", "f"),
                      ("g", "12"),
                      ("h", "h")], list(reader.read()))
    self.assertEqual(["g_missing_1"], reader.get_not_read())
  
  
  def readAll(self, reader):
    list(reader)
  
  def testReadFromInexistingLoShards(self):
    recordio_hi = RecordIOShard.create("test", lo="1")
    recordio_hi.insert(("1", STRING + "b"))
    recordio_hi.insert(("2", STRING + "c"))
    recordio_hi.commit()
    reader = RecordIOReader("test")
    self.assertRaises(RecordIOShardDoesNotExistError,
                      self.readAll, reader)

  def testReadFromInexistingHiShards(self):
    recordio_lo = RecordIOShard.create("test", hi="1")
    recordio_lo.insert(("0", STRING + "a"))
    recordio_lo.commit()
    reader = RecordIOReader("test")
    self.assertRaises(RecordIOShardDoesNotExistError,
                      self.readAll, reader)
    
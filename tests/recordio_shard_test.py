import unittest
import random

from recordio.recordio_shard import RecordIOShard, INTEGER_FMT, INTEGER_FMT_0, INTEGER_FMT_1
from recordio.recordio_records_zipped import ZIP_CHUNKS
from recordio.tests import test_helper

class RecordIOShardTestCase(unittest.TestCase):
  def setUp(self):
    test_helper.setUp(self)
  
  def tearDown(self):
    test_helper.tearDown(self)
  
  def getStrings(self):
    result = [""]
    for i in range(0, 128):
      result.append(str(unichr(i)))
      for j in range(0, 128):
        result.append(str(unichr(i) + unichr(j)))
    return result
  
  def getResult(self, gen):
    result = {}
    for shard_name, key_values in gen:
      result[shard_name] = key_values
    return result
  
  def testKeyName(self):
    name = RecordIOShard.key_name("te|st", ("b|b",), ("d|d",))
    self.assertEqual("te%7Cst!0!647c64!0000000000!0000000001!0000000000" +
                     "!627c62!0000000000!0000000001!0000000000", name)
    recordio = RecordIOShard.create("te|st", ("b|b", 0, 1, 0), ("d|d",))
    self.assertEqual("te|st", recordio.name())
    self.assertEqual((("b|b", 0, 1, 0),
                      ("d|d", 0, 1, 0)),
                     recordio.lo_hi())
  
  def insertGetAndOrder(self, compressed):
    recordio = RecordIOShard.create("test", compressed=compressed)
    recordio.insert(("a", "a"))

    test_strings = self.getStrings()
    assert(len(test_strings) > 1)
    random.shuffle(test_strings)
    for x in test_strings:
      recordio.insert((x, x))
    self.assertEqual(len(test_strings), len(recordio))
    for x in test_strings:
      recordio.insert((x, "".join(reversed(x))))
    self.assertEqual(len(test_strings), len(recordio))
    
    for i in range(0, len(test_strings), 500):
      x = test_strings[i]
      self.assertTrue(x in recordio)
      self.assertEqual(recordio[(x,)], "".join(reversed(x)))
    test_strings = self.getStrings()
    i = 0
    for key, value in recordio:
      self.assertEqual(test_strings[i], key)
      self.assertEqual("".join(reversed(test_strings[i])), value)
      i += 1
    assert("not_in" not in test_strings)
    self.assertFalse("not_in" in recordio)

  def testInsertGetAndOrderCompressed(self):
    self.insertGetAndOrder(True)
    
  def testInsertGetAndOrderUncompressed(self):
    self.insertGetAndOrder(False)

  def testSplit(self):
    recordio = RecordIOShard.create("test")
    test_strings = ["c", "a", "b", "d", "e"]
    for x in test_strings:
      recordio.insert((x, test_helper.uncompressableString(ZIP_CHUNKS)))
    lo_record, hi_record = recordio.split()
    self.assertEqual(3, len(lo_record))
    self.assertEqual(2, len(hi_record))
    for x in test_strings:
      self.assertTrue(x in lo_record or x in hi_record)
    self.assertTrue(max(lo_record) < min(hi_record))
    self.assertEqual("test", lo_record.name())
    self.assertEqual((None, ('d', 0, 1, 0)), lo_record.lo_hi())
    self.assertEqual(["a", "b", "c"], [x[0] for x in lo_record])
    self.assertEqual("test", hi_record.name())
    self.assertEqual((('d', 0, 1, 0), None),  hi_record.lo_hi())
    self.assertEqual(["d", "e"], [x[0] for x in hi_record])

  def testSplitEntriesSplit(self):
    recordio = RecordIOShard.create("test", compressed=False)
    recordio.insert(("b", 0, 3, 3, "bb"))
    recordio.insert(("b", 1, 3, 3, "bb"))
    recordio.insert(("b", 2, 3, 3, "bb"))
    lo_record, hi_record = recordio.split()
    self.assertEqual((None,  ('b', 2, 3, 3)),
                     lo_record.lo_hi())
    self.assertEqual((('b', 2, 3, 3), None), hi_record.lo_hi())

  def testGetAllQuery(self):
    RecordIOShard.create("test", hi=("a", "")).commit()
    RecordIOShard.create("test", lo=("a", ""), hi=("b", "")).commit()
    RecordIOShard.create("test", lo=("b", "")).commit()
    self.assertEqual(
        [(None, ("a", 0, 1, 0)),
         (('a', 0, 1, 0), ('b', 0, 1, 0)), 
         (('b', 0, 1, 0), None)],
        [RecordIOShard.lo_hi_from_key(x.name())
         for x in RecordIOShard.get_all_query("test", keys_only=True)])

  def testShardNamesForKeysNone(self):
    self.assertEqual({ None: [("0", ""), ("1", "")] },
                      self.getResult(RecordIOShard.get_shards_for_key_values(
                                     "test", [("0", ""), ("1", "")])))
    
  def testShardNamesForKeysEmpty(self):
    recordio = RecordIOShard.create("test")
    recordio.insert(("0", "a"))
    recordio.insert(("1", "b"))
    recordio.insert(("2", "c"))
    recordio.commit()
    self.assertEqual({ RecordIOShard.key_name("test"): [("", ),] },
                     self.getResult(RecordIOShard.get_shards_for_key_values(
                                    "test", [("",)])))

  def testShardNamesForShorterKeys(self):
    RecordIOShard.create("test", hi=("a", "")).commit()
    RecordIOShard.create("test", lo=("a", "")).commit()
    self.assertEqual({ RecordIOShard.key_name("test", lo=("a", "")):
                           [("aa", ),] },
                      self.getResult(RecordIOShard.get_shards_for_key_values(
                                     "test", [("aa",)])))

  def testShardNamesForKeysSplit(self):
    recordio = RecordIOShard.create("test")
    test_strings = [str(x) for x in range(10)]
    for x in test_strings:
      recordio.insert((x, test_helper.uncompressableString(2**16)))
    recordio.commit()
    self.assertEqual({ RecordIOShard.key_name("test"):
                           [("0", ""), ("1", "")] },
                     self.getResult(RecordIOShard.get_shards_for_key_values(
                                    "test", [("0", ""), ("1", "")])))
    recordio.delete()
    shard_0, shard_1 = recordio.split()
    shard_1, shard_2 = shard_1.split()
    shard_0.commit()
    shard_1.commit()
    shard_2.commit()
    self.assertEqual({ shard_0.key().name(): [('0', '0'), ('1', '1'),
                                              ('2', '2'), ('3', '3'),
                                              ('4', '4')],
                      shard_1.key().name(): [('5', '5'), ('6', '6'),
                                             ('7', '7')],
                      shard_2.key().name(): [('8', '8'), ('9', '9')]},
                      self.getResult(RecordIOShard.get_shards_for_key_values(
                                     "test", zip(test_strings, test_strings))))

  def testShardNamesForKeysMissingLo(self):
    recordio_hi = RecordIOShard.create("test", lo="1")
    recordio_hi.insert(("1", "b"))
    recordio_hi.insert(("2", "c"))
    recordio_hi.commit()
    self.assertEqual({ None: [("0", )] },
                     self.getResult(RecordIOShard.get_shards_for_key_values(
                                    "test", [("0", )])))
  def testShardNamesForKeysMissingHi(self):
    recordio_lo = RecordIOShard.create("test", hi="1")
    recordio_lo.insert(("0", "a"))
    recordio_lo.commit()
    self.assertEqual({ None: [("1", )] },
                     self.getResult(RecordIOShard.get_shards_for_key_values(
                                    "test", [("1", )])))
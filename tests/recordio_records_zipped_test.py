import unittest
import marshal

from recordio.recordio_records_zipped import RecordIORecordsZipped, ZIP_CHUNKS
from recordio.tests import test_helper

class RecordIORecordsZippedTestCase(unittest.TestCase):
  def setUp(self):
    test_helper.setUp(self)
  
  def tearDown(self):
    test_helper.tearDown(self)
  
  def getResult(self, gen):
    result = {}
    last_entry = None
    for entry in gen:
      if last_entry:
        self.assertTrue(last_entry < entry)
      last_entry = entry
      result[entry[0]] = entry[1:]
    return result

  def insertABC(self, records):
    records.insert(("c", "cc"))
    records.insert(("a", "aa"))
    records.insert(("b", "bb"))

  def testGetData(self):
    data = [("a", "aa"), ("b", "bb")]
    records = RecordIORecordsZipped()
    records.insert(data[0])
    records.insert(data[1])
    records = RecordIORecordsZipped(records.get_data())
    self.assertEqual(list(records), data)

  
  def testInsertGetAndRead(self):
    records = RecordIORecordsZipped()
    self.insertABC(records)
    self.assertEqual(len(records), 3)
    self.assertEqual({"a": ("aa",), "b": ("bb",), "c": ("cc",)},
                     self.getResult(records))
    records = RecordIORecordsZipped(records.get_data())
    records.insert(("b", "new"))
    self.assertEqual(len(records), 3)
    self.assertEqual(records["b"], ("b", "new"))
    self.assertTrue("a" in records)
    self.assertFalse("z" in records)
    records = RecordIORecordsZipped(records.get_data())
    records.insert(("b", "bb"))
    self.assertEqual({"a": ("aa",), "b": ("bb",), "c": ("cc",)},
                     self.getResult(records.read()))
    self.assertEqual({"a": ("aa",), "b": ("bb",), "c": ("cc",)},
                     self.getResult(records.read(("",), ("d",))))
    self.assertEqual({"b": ("bb",)},
                     self.getResult(records.read(("b",), ("c",))))
  
  def testDelete(self):
    records = RecordIORecordsZipped()
    self.insertABC(records)
    records = RecordIORecordsZipped(records.get_data())
    records.insert(("b",))
    records = RecordIORecordsZipped(records.get_data())
    self.assertEqual({"a": ("aa",), "c": ("cc",)},
                     self.getResult(records))

  def testSplit(self):
    records = RecordIORecordsZipped()
    self.insertABC(records)
    records.insert(("d", test_helper.uncompressableString(ZIP_CHUNKS/2)))
    records.insert(("e", test_helper.uncompressableString(ZIP_CHUNKS)))
    lo, hi, middle = records.split()
    lo = RecordIORecordsZipped(lo)
    hi = RecordIORecordsZipped(hi)
    self.assertEqual(middle[0], "e")
    self.assertEqual(["a", "b", "c", "d"],
                     list(sorted(self.getResult(lo).keys())))
    self.assertEqual({"a": ("aa",), "b": ("bb",), "c": ("cc",)},
                     self.getResult(lo.read(("",), ("d",))))
    self.assertEqual(["e"], self.getResult(hi).keys())

  def testInsertSplitDataSmallToBig(self):
    records = RecordIORecordsZipped()
    self.insertABC(records)
    records = RecordIORecordsZipped(records.get_data())
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    records = RecordIORecordsZipped(records.get_data())
    self.assertEqual([("a", "aa"),
                      ("b", 0, 3, 3, "bb"),
                      ("b", 1, 3, 3, "bb"),
                      ("b", 2, 3, 3, "bb"),
                      ("c", "cc")],
                     list(records))
    
  def testInsertSplitDataBigToSmall(self):
    records = RecordIORecordsZipped()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    records.insert(("c", "cc"))
    records = RecordIORecordsZipped(records.get_data())
    records.insert(("b", "bb"))
    records = RecordIORecordsZipped(records.get_data())
    self.assertEqual([("a", "aa"), ("b", "bb"), ("c", "cc")],
                     list(records))

  def testInsertSplitDataBigToBigger(self):
    records = RecordIORecordsZipped()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 2, 2, "bb"))
    records.insert(("b", 1, 2, 2, "bb"))
    records.insert(("c", "cc"))
    records = RecordIORecordsZipped(records.get_data())
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    records = RecordIORecordsZipped(records.get_data())
    self.assertEqual([("a", "aa"),
                      ("b", 0, 3, 3, "bb"),
                      ("b", 1, 3, 3, "bb"),
                      ("b", 2, 3, 3, "bb"),
                      ("c", "cc")],
                     list(records))
  
  def testInsertSplitDataBiggerToBig(self):
    records = RecordIORecordsZipped()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    records.insert(("c", "cc"))
    records = RecordIORecordsZipped(records.get_data())
    records.insert(("b", 0, 2, 2, "bb"))
    records.insert(("b", 1, 2, 2, "bb"))
    records = RecordIORecordsZipped(records.get_data())
    self.assertEqual([("a", "aa"),
                      ("b", 0, 2, 2, "bb"),
                      ("b", 1, 2, 2, "bb"),
                      ("c", "cc")],
                     list(records))
  
  def testInsertSplitDataBiggerToBigToSmall(self):
    records = RecordIORecordsZipped()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    records.insert(("c", "cc"))
    records = RecordIORecordsZipped(records.get_data())
    records.insert(("b", 0, 2, 2, "bb"))
    records.insert(("b", 1, 2, 2, "bb"))
    records.insert(("b", "bb"))
    records = RecordIORecordsZipped(records.get_data())
    self.assertEqual([("a", "aa"), ("b", "bb"), ("c", "cc")],
                     list(records))
  
  def testInsertNotDeleted(self):
    records = RecordIORecordsZipped()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 3, 3, "bb"))
    records = RecordIORecordsZipped(records.get_data())
    records.insert(("b", 0, 2, 2, "bb"))
    other = RecordIORecordsZipped(records.get_data())
    self.assertEqual([("a", "aa"), ("b", 0, 2, 2, "bb")],
                     list(other))
    self.assertEqual([('b', 1, 3, 3),
                      ('b', 2, 3, 3)],
                     list(records.not_deleted()))

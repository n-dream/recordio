import unittest
import marshal

from recordio.recordio_records import RecordIORecords
from recordio.tests import test_helper

class RecordIORecordsTestCase(unittest.TestCase):
  def setUp(self):
    test_helper.setUp(self)
  
  def tearDown(self):
    test_helper.tearDown(self)

  def insertABC(self, records):
    records.insert(("c", "cc"))
    records.insert(("a", "aa"))
    records.insert(("b", "bb"))

  def testGetData(self):
    data = [("a", "aa"), ("b", "bb")]
    records = RecordIORecords()
    records.insert(data[0])
    records.insert(data[1])
    records = RecordIORecords(records.get_data())
    self.assertEqual(list(records), data)

  def testInsertGetAndRead(self):
    records = RecordIORecords()
    self.insertABC(records)
    self.assertEqual(len(records), 3)
    self.assertEqual([("a", "aa"), ("b", "bb"), ("c", "cc")],
                     list(records))
    records.insert(("b", "new"))
    self.assertEqual(len(records), 3)
    self.assertEqual(records["b"], ("b", "new"))
    self.assertTrue("a" in records)
    self.assertFalse("z" in records)
    records.insert(("b", "bb"))
    self.assertEqual([("a", "aa"), ("b", "bb"), ("c", "cc")],
                     list(records.read()))
    self.assertEqual([("a", "aa"), ("b", "bb"), ("c", "cc")],
                     list(records.read(("",), ("d",))))
    self.assertEqual([("b", "bb")],
                     list(records.read(("b",), ("c",))))
  
  def testDelete(self):
    records = RecordIORecords()
    self.insertABC(records)
    self.assertTrue(records.insert(("b",)))
    records.insert(("b", "bb"))
    self.assertEqual([("a", "aa"), ("b", "bb"), ("c", "cc")],
                     list(records))
    self.assertTrue(records.insert(("b",)))
    self.assertFalse(records.insert(("d",)))
    records = RecordIORecords(records.get_data())
    self.assertEqual([("a", "aa"), ("c", "cc")],
                     list(records))
  
  def testSplit(self):
    records = RecordIORecords()
    self.insertABC(records)
    records.insert(("d", "dd"))
    records.insert(("e", "ee"))
    lo, hi, middle = records.split()
    lo = RecordIORecords(lo)
    hi = RecordIORecords(hi)
    self.assertEqual(middle, ("d", "dd"))
    self.assertEqual([("a", "aa"), ("b", "bb"), ("c", "cc")],
                     list(lo))
    self.assertEqual([("d", "dd"), ("e", "ee")], list(hi))

  def testComperator(self):
    self.assertEqual(RecordIORecords.entry_comperator(
        ("a",), ("b",)), -1)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b",), ("b",)), 0)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("c",), ("b",)), 1)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b", "bb"), ("b",)), 0)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b", 0, 1, 1, "bb"), ("b",)), 0)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b",), ("b", 0, 1, 1, "bb")), 0)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b", 0, 1, 1, "bb"), ("b", "bb")), 0)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b", "bb"), ("b", 0, 1, 1, "bb")), 0)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b", 0, 1, 1, "bb"),
        ("b", 0, 1, 2, "bb")), 0)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b", 1, 2, 1, "bb"),
        ("b", 1, 2, 2, "bb")), -1)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b", 1, 2, 1, "bb"),
        ("b", 1, 2, 1, "bb")), 0)
    self.assertEqual(RecordIORecords.entry_comperator(
        ("b", 1, 3, 1, "bb"),
        ("b", 1, 2, 1, "bb")), 1)
  
  def testInRange(self):
    self.assertTrue(RecordIORecords.in_range(("a", )))
    self.assertTrue(RecordIORecords.in_range(("a", ), lo=("a",)))
    self.assertTrue(RecordIORecords.in_range(("a", ), hi=("b",)))
    self.assertTrue(RecordIORecords.in_range(("b", ), lo=("a",), hi=("c",)))
    self.assertTrue(RecordIORecords.in_range(("a", ), lo=("a",), hi=("b",)))
    self.assertFalse(RecordIORecords.in_range(("a", ), lo=("b",)))
    self.assertFalse(RecordIORecords.in_range(("b", ), hi=("b",)))


  def testInsertSplitDataSmallToBig(self):
    records = RecordIORecords()
    self.insertABC(records)
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    self.assertEqual([("a", "aa"),
                      ("b", 0, 3, 3, "bb"),
                      ("b", 1, 3, 3, "bb"),
                      ("b", 2, 3, 3, "bb"),
                      ("c", "cc")],
                     list(records))
    
  def testInsertSplitDataBigToSmall(self):
    records = RecordIORecords()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    records.insert(("c", "cc"))
    records.insert(("b", "bb"))
    records = RecordIORecords(records.get_data())
    self.assertEqual([("a", "aa"), ("b", "bb"), ("c", "cc")],
                     list(records))

  def testInsertSplitDataBigToBigger(self):
    records = RecordIORecords()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 2, 2, "bb"))
    records.insert(("b", 1, 2, 2, "bb"))
    records.insert(("c", "cc"))
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    records = RecordIORecords(records.get_data())
    self.assertEqual([("a", "aa"),
                      ("b", 0, 3, 3, "bb"),
                      ("b", 1, 3, 3, "bb"),
                      ("b", 2, 3, 3, "bb"),
                      ("c", "cc")],
                     list(records))
  
  def testInsertSplitDataBiggerToBig(self):
    records = RecordIORecords()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    records.insert(("c", "cc"))
    records.insert(("b", 0, 2, 2, "bb"))
    records.insert(("b", 1, 2, 2, "bb"))
    records = RecordIORecords(records.get_data())
    self.assertEqual([("a", "aa"),
                      ("b", 0, 2, 2, "bb"),
                      ("b", 1, 2, 2, "bb"),
                      ("c", "cc")],
                     list(records))
  
  def testInsertSplitDataBiggerToBigToSmall(self):
    records = RecordIORecords()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 1, 3, 3, "bb"))
    records.insert(("b", 2, 3, 3, "bb"))
    records.insert(("c", "cc"))
    records.insert(("b", 0, 2, 2, "bb"))
    records.insert(("b", 1, 2, 2, "bb"))
    records.insert(("b", "bb"))
    records = RecordIORecords(records.get_data())
    self.assertEqual([("a", "aa"), ("b", "bb"), ("c", "cc")],
                     list(records))
  
  def testInsertNotDeleted(self):
    records = RecordIORecords()
    records.insert(("a", "aa"))
    records.insert(("b", 0, 3, 3, "bb"))
    records.insert(("b", 0, 2, 2, "bb"))
    other = RecordIORecords(records.get_data())
    self.assertEqual([("a", "aa"), ("b", 0, 2, 2, "bb")],
                     list(other))
    self.assertEqual([('b', 1, 3, 3),
                      ('b', 2, 3, 3)],
                     list(records.not_deleted()))
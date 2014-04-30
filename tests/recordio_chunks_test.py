import unittest
import random

from recordio.recordio_chunks import get_chunks
from recordio.tests import test_helper

class RecordIOShardTestCase(unittest.TestCase):
  def setUp(self):
    test_helper.setUp(self)
  
  def tearDown(self):
    test_helper.tearDown(self)

  def testGetChunks(self):
    self.assertEqual([[("a", "a")]], list(get_chunks([("a", "a")], 2**16)))
    self.assertEqual([[("a", "a")]], list(get_chunks([("a", "a")], 1)))
    a_100 = "a" * 100
    self.assertEqual([[("a", a_100)],
                      [("a", a_100)],],
                     list(get_chunks([("a", a_100), ("a", a_100)], 150)))
    self.assertEqual([[("a", a_100), ("a", a_100)],
                      [("a", a_100), ("a", a_100)]],
                     list(get_chunks([("a", a_100), ("a", a_100),
                                     ("a", a_100), ("a", a_100)], 300)))
import unittest

from recordio.bisect_left import bisect_left

class BisectLeftCase(unittest.TestCase):
  def testBisectLeftEmpty(self):
    self.assertEqual(bisect_left([], 0), 0)

  def testBisectLeftOneElement(self):
    self.assertEqual(bisect_left([0], 0), 0)
    self.assertEqual(bisect_left([0], -1), 0)
    self.assertEqual(bisect_left([0], 1), 1)

  def testBisectLeftFullEven(self):
    test_array = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    for i in xrange(0, 10):
      self.assertEqual(bisect_left(test_array, i), i)
    self.assertEqual(bisect_left(test_array, -1), 0)
    self.assertEqual(bisect_left(test_array, 10), 10)
  
  def testBisectLeftFullOdd(self):
    test_array = [0, 1, 2, 3, 4, 5, 6, 7, 8]
    for i in xrange(0, 9):
      self.assertEqual(bisect_left(test_array, i), i)

  def testBisectLeftSparseEven(self):
    test_array = [0, 2, 4, 6, 8, 10]
    for i in xrange(0, 10):
      self.assertEqual(bisect_left(test_array, i), (i+1)/2)

  def testBisectLeftSparseOdd(self):
    test_array = [0, 2, 4, 6, 8]
    for i in xrange(0, 9):
      self.assertEqual(bisect_left(test_array, i), (i+1)/2)
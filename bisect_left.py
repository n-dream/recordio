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

def default_compare(a, b):
  """Default comperator.

  :param a: First element
  :param b: Second element
  :return: Integer
  """
  if a < b:
    return -1
  elif a == b:
    return 0
  else:
    return 1

def bisect_left(array, value, comperator=default_compare):
  """Same as python's bisect.bisect_left but with a custom comperator.

  :param array: The sorted list to search
  :param value: The value to be inserted
  :param comperator: A comperator function.
  :return: A position to be used with list.insert()
  """
  if len(array) == 0:
    return 0
  comparison = comperator(value, array[-1])
  if comparison == 0:
    return len(array) - 1
  elif comparison > 0:
    return len(array)
  comparison = comperator(value, array[0])
  if comparison <= 0:
    return 0
  
  left = 0
  right = len(array)-1
  while left <= right: 
    middle = (left + right) // 2
    comparison = comperator(value, array[middle])
    if comparison < 0:
      right = middle - 1
    elif comparison > 0:
      left = middle + 1
    else:
      return middle
  return left
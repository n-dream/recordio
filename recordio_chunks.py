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

KEY_VALUE_OVERHEAD = 5

def size(entry):
  """Estimates the size of RecordIO entry.

  :param entry: A tuple of some length.
  :return: length in bytes
  """
  length = KEY_VALUE_OVERHEAD
  for x in entry:
    if isinstance(x, int):
      length += KEY_VALUE_OVERHEAD
    elif isinstance(x, long):
      length += KEY_VALUE_OVERHEAD*3
    else:
      length += len(x) + KEY_VALUE_OVERHEAD
  return length

def get_chunks(updates, max_size):
  """Given a list of RecordIO entry tuples, returns chunks of that list.

  :param updates: A list of RecordIO entry tuples.
  :param max_size: The maximum allowed chunk size.
  :return: Yields lists RecordIO entry tuples which don't exceed max_size.
  """
  chunk = []
  current_size = 0
  for update in updates:
    entry_size = size(update)
    if current_size + entry_size >= max_size:
      if chunk:
        yield chunk
      chunk = [update]
      current_size = entry_size
    else:
      chunk.append(update)
      current_size += entry_size
  if chunk:
    yield chunk
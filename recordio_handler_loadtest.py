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

from google.appengine.ext.webapp import template

import os
import webapp2
import random

from recordio.recordio_writer import RecordIOWriter
from recordio.recordio_reader import RecordIOReader
import time
import logging

class StringGenerator():
  def __init__(self, compressable):
    n = { "15%": 1,
          "25%": 5,
          "50%": 54,
          "75%": 160 }[compressable]
    self.data = "".join([chr(random.randint(0, n))*2 for x in xrange(2**15)])
    self.pos = 0
  
  def next(self, length):
    result = []
    result_length = 0
    while result_length < length:
      if self.pos >= len(self.data):
        self.pos = 0
      chunk = self.data[self.pos:self.pos + length - result_length]
      self.pos += len(chunk)
      result_length += len(chunk)
      result += [chunk]
    return "".join(result)
      
class LoadTestHandler(webapp2.RequestHandler):
  """A handler to loadtest RecordIOs"""
  def handle(self, values):
    try:
      import gae_mini_profiler
      values["gae_profiler_includes"] = (
          gae_mini_profiler.templatetags.profiler_includes())
    except:
      pass
    values["entries"] = self.request.get("entries", "100")
    values["entry_size_min"] = self.request.get("entry_size_min", "0")
    values["entry_size_max"] = self.request.get("entry_size_max", "1024")
    values["entry_size_key"] = self.request.get("entry_size_key", "100000")
    values["compressable"] = self.request.get("compressable", "50%")
    self.response.out.write(
        template.render(os.path.join(os.path.dirname(__file__),
                        'recordio_handler_loadtest.html'), values))
  
  def get(self):
    values = {}
    values["run_compressed"] = "checked"
    values["run_uncompressed"] = "checked"
    self.handle(values)
    
  
  def do_write(self, single, compressed, entries):
    start = time.time()
    writer = RecordIOWriter("loadtest_" + single + "_" +
                            { True: "compressed",
                              False: "uncompressed"}[compressed])
    writer.create(compressed=compressed)
    for entry in entries:
      writer.insert(entry[0], entry[1])
    writer.commit_sync(retries=10)
    return time.time() - start, writer.db_stats()
  
  def do_read(self, single, compressed, entries):
    to_read = set([x[0] for x in entries])
    last_read = None
    start = time.time()
    reader = RecordIOReader("loadtest_" + single + "_" +
                            { True: "compressed",
                              False: "uncompressed"}[compressed])
    for key, value in reader:
      if last_read and last_read >= key:
        raise Exception("Results not returned in order! %s >= %s" %
                        (last_read, key))
      last_read = key
      if key in to_read:
        to_read.remove(key)
    if len(to_read):
      raise NotEveryThingWrittenError
    return time.time() - start, reader.db_stats()
  
  def post(self):
    values = {}
    if self.request.get("run_uncompressed"):
      values["run_uncompressed"] = "checked"
    if self.request.get("run_compressed"):
      values["run_compressed"] = "checked"
    if self.request.get("delete"):
      RecordIOWriter("loadtest_single_compressed").delete()
      RecordIOWriter("loadtest_single_uncompressed").delete()
      RecordIOWriter("loadtest_combined_compressed").delete()
      RecordIOWriter("loadtest_combined_uncompressed").delete()
      self.handle(values)
      return
    amount = int(self.request.get("entries"))
    entry_size_min = int(self.request.get("entry_size_min"))
    entry_size_max = int(self.request.get("entry_size_max"))
    entry_size_key = int(self.request.get("entry_size_key"))
    compressable = self.request.get("compressable")
    entries = []
    gen = StringGenerator(compressable)
    for i in xrange(amount):
      entries.append((str(random.randint(0, entry_size_key)),
                      gen.next(random.randint(entry_size_min,
                                              entry_size_max))))
    values["ran"] = True
    single = "single"
    if (self.request.get("run_uncompressed") and
        self.request.get("run_compressed")):
      single = "combined"
      
    if self.request.get("run_uncompressed"):
      logging.info("Starting uncompressed write loadtest")
      values["write_uncompressed"] = self.do_write(single, False, entries)
    if self.request.get("run_compressed"):
      logging.info("Starting compressed write loadtest")
      values["write_compressed"] = self.do_write(single, True, entries)
    try:
      if self.request.get("run_uncompressed"):
        logging.info("Starting uncompressed read loadtest")
        values["read_uncompressed"] = self.do_read(single, False, entries)
      if self.request.get("run_compressed"):
        logging.info("Starting compressed read loadtest")
        values["read_compressed"] = self.do_read(single, True, entries)
    except NotEveryThingWrittenError:
      logging.info("Maybe not ready to read!")
      time.sleep(5)
      if self.request.get("run_uncompressed"):
        logging.info("Starting uncompressed read loadtest")
        values["read_uncompressed"] = self.do_read(single, False, entries)
      if self.request.get("run_compressed"):
        logging.info("Starting compressed read loadtest")
        values["read_compressed"] = self.do_read(single, True, entries)
      
    logging.info("Loadtests done")
    self.handle(values)

class NotEveryThingWrittenError(Exception):
  pass
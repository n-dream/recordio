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
import urllib

from recordio.recordio_shard import RecordIOShard
from recordio.recordio_writer import RecordIOWriter
from recordio.recordio_reader import RecordIOReader

ENTRIES_PER_PAGE = 10
      
class EditorHandler(webapp2.RequestHandler):
  """The RecordIO Viewer. Available at /recordio/"""
  def get(self):
    values = {}
    name = self.request.get("name")
    if name:
      values["name"] = name
      start = self.request.get("start", "")
      values["start"] = start
      start_key = str(start)
      key_values = [(key, repr(value)) for key, value in
                     RecordIOReader(name).read(
                         start_key=start_key,
                         limit=ENTRIES_PER_PAGE+1)]
      if len(key_values) == ENTRIES_PER_PAGE + 1:
        values["next"] = key_values[ENTRIES_PER_PAGE][0]
      values["key_values"] = key_values[:ENTRIES_PER_PAGE]
    else:
      values["names"] = list(RecordIOReader.all_names())
    self.response.out.write(
        template.render(os.path.join(os.path.dirname(__file__),
                        'recordio_handler_editor.html'), values))
  
  
  def post(self):
    name = self.request.get("name")
    compressed = not not self.request.get("compressed")
    key = self.request.get("key", None)
    value = self.request.get("value", None)
    if name:
      writer = RecordIOWriter(name)
      if key == None and value == None:
        writer.create(compressed)
      elif value == None:
        writer.remove(key)
        writer.commit_sync()
      else:
        writer.insert(str(key), eval(value))
        writer.commit_sync()
      start = None
      if key:
        start = str(key)
      self.redirect("?name=" + str(urllib.quote(name)) + "&start=" +
                    urllib.quote(start))
    delete = self.request.get("delete")
    if delete:
      writer = RecordIOWriter(delete)
      writer.delete()
      self.redirect("/recordio/")
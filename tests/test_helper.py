from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util
import random
import os
import webapp2

def setUp(self):
  self.testbed = testbed.Testbed()
  self.testbed.activate()
  policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
  self.testbed.init_datastore_v3_stub(consistency_policy=policy)
  self.testbed.init_memcache_stub()
  self.testbed.init_taskqueue_stub(root_path=os.path.dirname( __file__ ))

def tearDown(self):
  self.testbed.deactivate()

random.seed(0)
uncompressable_65k = "".join([chr(random.randint(0,255)) for x in range(2**16)])

def uncompressableString(length):
  result = uncompressable_65k
  while len(result) < length:
    result += uncompressable_65k
  return result[:length]

def compressableString(length):
  return "a" * length

def keyGenerator():
  for a in xrange(255):
    yield chr(a)
    for b in xrange(255):
      yield chr(a) + chr(b)
      for c in xrange(255):
        yield chr(a) + chr(b) + chr(c)
  raise Exception("Ran out of keys")

def requestInitialize(handler, url):
  request_vars = webapp2.Request.blank(url)
  handler.initialize(request_vars, webapp2.Response())
  setattr(handler, "is_initialized", True)

def requestGetOrPost(handler, method="GET", url='', args=None):
  if not hasattr(handler, "is_initialized"):
    requestInitialize(handler, url)
  handler.request.method = method
  handler.request.remote_addr = "127.0.0.1"
  if args:
    getattr(handler.request, method).update(args)
  getattr(handler, method.lower())()
  return handler.response.unicode_body # Python2.7

def requestGet(handler, url='', args=None):
  return requestGetOrPost(handler, "GET", url, args)

def requestPost(handler,  url='', args=None):
  return requestGetOrPost(handler, "POST", url, args)
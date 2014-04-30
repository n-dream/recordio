#!/usr/bin/python
import unittest
import os
import sys

def main():
  sdk_path = "/Applications/GoogleAppEngineLauncher.app/Contents/Resources/GoogleAppEngine-default.bundle/Contents/Resources/google_appengine"
  sys.path.insert(0, sdk_path)
  sys.path.insert(0, os.getcwd())
  import dev_appserver
  dev_appserver.fix_sys_path()
  pattern = "*_test.py"
  if len(sys.argv) == 2:
    pattern = sys.argv[1]
  suite = unittest.loader.TestLoader().discover("recordio/tests", pattern=pattern)
  unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
    main()
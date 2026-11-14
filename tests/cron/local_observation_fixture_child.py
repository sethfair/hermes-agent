"""Harmless, explicitly selected unittest child for launcher tests."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

class ChildTests(unittest.TestCase):
    def test_child(self):
        from local_observation_retained_route import LINEAGE_KEYS
        mode=os.environ['WRI_ROUTE_MODE']
        child=None
        try:
            if mode=='timeout': child=subprocess.Popen([sys.executable,'-B',str(Path(__file__).resolve()),'sleep'])
            data={'lineage':{k:os.environ.get(k) for k in LINEAGE_KEYS},
                  'tmpdir':tempfile.gettempdir(),'mode':Path(tempfile.gettempdir()).stat().st_mode & 0o777,
                  'pgid':os.getpgrp(),'grandchild':child.pid if child else None}
            Path(os.environ['WRI_ROUTE_REPORT']).write_text(json.dumps(data))
            if mode=='timeout': threading.Event().wait()
            self.assertNotEqual('fail',mode,'intentional nonzero child')
        finally:
            if child is not None:
                child.terminate(); child.wait(timeout=5)

if __name__=='__main__':
    if sys.argv[1:] == ['sleep']: threading.Event().wait()
    else: unittest.main()

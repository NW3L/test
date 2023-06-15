import os
import time

os.execv("test/test1.py", ["test/test1.py"])
time.sleep(60)
os.execv("test/test2.py", ["test/test2.py"])
time.sleep(120)
os.execv("test/test3.py", ["test/test3.py"])
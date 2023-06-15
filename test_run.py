import os

os.system("python3.10 test/test1.py")

if os.WEXITSTATUS(os.system("echo $?")) == 0:
    os.system("python3.10 test/test2.py")

if os.WEXITSTATUS(os.system("echo $?")) == 0:
    os.system("python3.10 test/test3.py")

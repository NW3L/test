import os
import time
# запускаем test1.py
os.system("python3.10 test/test1.py")
time.sleep(60)

# запускаем test2.py, если test1.py завершился успешно
if os.WEXITSTATUS(os.system("echo $?")) == 0:
    os.system("python3.10 test/test2.py")
time.sleep(120)

# запускаем test3.py, если test2.py завершился успешно
if os.WEXITSTATUS(os.system("echo $?")) == 0:
    os.system("python3.10 test/test3.py")

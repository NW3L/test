import os
import time

# запускаем test1.py
os.system("python3.10 sunday_update/month_d1_download.py")
time.sleep(120)

# запускаем test2.py, если test1.py завершился успешно
if os.WEXITSTATUS(os.system("echo $?")) == 0:
    os.system("python3.10 sunday_update/month_d1_update.py")

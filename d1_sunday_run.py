import os
import time

os.system("python3.10 d1_sunday_update/month_d1_download.py")
time.sleep(120)

if os.WEXITSTATUS(os.system("echo $?")) == 0:
    os.system("python3.10 d1_sunday_update/month_d1_download.py")

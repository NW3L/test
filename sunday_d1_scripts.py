import os
import time
# d1 all download
os.system("python3.10 test/0_month_d1_download.py")
time.sleep(120)

# d1 base update
if os.WEXITSTATUS(os.system("echo $?")) == 0:
    os.system("python3.10 test/0_month_d1_update.py")

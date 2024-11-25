from time import sleep, time
from json import dump
import subprocess


P3 = subprocess.Popen(['python', 'File1.py', "4"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
stdout, stderr = P3.communicate()

if P3.returncode == 0:
    print(stdout)
else:
    print(stderr)
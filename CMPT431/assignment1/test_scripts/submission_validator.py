#!/usr/bin/env python

import os
import re
import argparse
import subprocess
import time


uniqueString = "{}".format(int(time.time() * 1000000))
scratchFolder = "/tmp/folder_"
scratchFolderPath = scratchFolder+uniqueString

parser = argparse.ArgumentParser()
parser.add_argument('--tarPath', help='Enter the full Path of the page_rank object file')

args = parser.parse_args()
tarFile = args.tarPath

print("Evaluating " + tarFile)

initialPath = os.getcwd()
tempDir = scratchFolderPath


if os.path.isdir(tempDir):
    commandArray = ["rm", "-r", tempDir]
    res = subprocess.call(commandArray)

commandArray = ["mkdir", tempDir]
print(" ".join(commandArray))
res = subprocess.call(commandArray)
if res != 0:
    print("Unable to mkdir. Ensure that you run the python script from a directory where you have write permissions")
    exit(1)

os.chdir(tempDir)

commandArray = ["tar", "xzf", tarFile]
res = subprocess.call(commandArray)
if res != 0:
    print("Unable to unzip")
    exit(1)

executables = ["producer_consumer"]

flag = False
for curr in executables:
    commandArray = ["make", curr]
    res = subprocess.call(commandArray)
    if res != 0:
        flag = True
        print("Unable to make {}".format(curr))  
        continue

    # check if executable is present
    commandArray = ["ls", curr]
    res = subprocess.call(commandArray)
    if res != 0:
        flag = True
        print("Unable to find {} executable".format(curr))  


os.chdir(tempDir)
commandArray = ["rm", "-r", tempDir]
res = subprocess.call(commandArray)

if flag == True:
    print("Submission incorrect. Check the folder structure")
else:
    print("Submission validated. Folder structure is as expected.")


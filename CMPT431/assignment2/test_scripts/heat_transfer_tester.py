#!/usr/bin/env python

import sys
import os
import re
import argparse
import subprocess
import time


uniqueString = "{}".format(int(time.time() * 1000000))
scratchFolder = "/tmp/scratch_"

parser = argparse.ArgumentParser()
parser.add_argument('--execPath', help='Enter the full Path of the heat_transfer_parallel object file')
parser.add_argument('--scriptPath', help='Enter the full Path of the evaluation script', nargs='?', default="/scratch/assignment2/test_scripts/heat_transfer_evaluator.py", const="")

args = parser.parse_args()
execPath = args.execPath
evaluationScriptPath = args.scriptPath
scratchFilePath = scratchFolder+uniqueString

print("Evaluating " + execPath)

#Testcase 1---------------------------------------------------------------------------------------
print("Executing Testcase1")
expectedResult = 799.5
nThreads = 4
serialTime = 11
parallelTime = 3
mTemp = 800
tSteps = 500
iCX = 0.26
iCY = 0.25
gSize = 1000

print("Using {} threads".format(nThreads))
scratchFile = open(scratchFilePath, 'w')
commandArray = [execPath, "--nThreads", str(nThreads), "--tSteps", str(tSteps), "--gSize", str(gSize), "--mTemp", str(mTemp), "--iCX", str(iCX), "--iCY", str(iCY)]
print(" ".join(commandArray))
res = subprocess.call(commandArray, stdout=scratchFile)
scratchFile.close()
print("Test case 1 exited with exit code : {}".format(res))
print("Evaluating Test case 1...")
sys.stdout.flush()

commandArray = ["python", evaluationScriptPath, "--file", scratchFilePath, "--nThreads", str(nThreads), "--expectedResult", str(expectedResult), "--serialTime", str(serialTime), "--parallelTime", str(parallelTime)]
# print(" ".join(commandArray))
res = subprocess.call(commandArray)
print("Evaluation script exited with exit code : {}".format(res))

#Testcase 2---------------------------------------------------------------------------------------
print("Executing Testcase2")
expectedResult = 578.09
nThreads = 4
serialTime = 76
parallelTime = 21
mTemp = 600
tSteps = 4000
iCX = 0.1
iCY = 0.15
gSize = 1000

print("Using {} threads".format(nThreads))
scratchFile = open(scratchFilePath, 'w')
commandArray = [execPath, "--nThreads", str(nThreads), "--tSteps", str(tSteps), "--gSize", str(gSize), "--mTemp", str(mTemp), "--iCX", str(iCX), "--iCY", str(iCY)]
print(" ".join(commandArray))
res = subprocess.call(commandArray, stdout=scratchFile)
scratchFile.close()
print("Test case 2 exited with exit code : {}".format(res))
print("Evaluating Test case 2...")
sys.stdout.flush()

commandArray = ["python", evaluationScriptPath, "--file", scratchFilePath, "--nThreads", str(nThreads), "--expectedResult", str(expectedResult), "--serialTime", str(serialTime), "--parallelTime", str(parallelTime)]
# print(" ".join(commandArray))
res = subprocess.call(commandArray)
print("Evaluation script exited with exit code : {}".format(res))

#Deleting scratchFile -----------------------------------------------------------------------------
commandArray = ["rm", scratchFilePath]
# print(" ".join(commandArray))
res = subprocess.call(commandArray)

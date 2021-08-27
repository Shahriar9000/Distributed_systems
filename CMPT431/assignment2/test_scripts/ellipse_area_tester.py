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
parser.add_argument('--execPath', help='Enter the full Path of the ellipse_area_parallel object file')
parser.add_argument('--scriptPath', help='Enter the full Path of the evaluation script', nargs='?', default="/scratch/assignment2/test_scripts/ellipse_area_evaluator.py", const="")

args = parser.parse_args()
execPath = args.execPath
evaluationScriptPath = args.scriptPath
scratchFilePath = scratchFolder+uniqueString

print("Evaluating " + execPath)

#Testcase 1---------------------------------------------------------------------------------------
print("Executing Testcase1")
nPoints = 500000007
expectedResult = nPoints
nThreads = 4
serialTime = 23
parallelTime = 6.5
majorR = 2.0
minorR = 1.0

print("Using {} threads".format(nThreads))
scratchFile = open(scratchFilePath, 'w')
commandArray = [execPath, "--nThreads", str(nThreads), "--nPoints", str(nPoints), "--majorRad", str(majorR), "--minorRad", str(minorR)]
print(" ".join(commandArray))
res = subprocess.call(commandArray, stdout=scratchFile)
scratchFile.close()
print("Test case 1 exited with exit code : {}".format(res))
print("Evaluating Test case 1...")
sys.stdout.flush()
# print("Reading scratchFile...")
# with open(scratchFilePath, 'r') as scratchFile:
#     contents = scratchFile.read()
#     print(contents)

commandArray = ["python", evaluationScriptPath, "--file", scratchFilePath, "--nThreads", str(nThreads), "--expectedResult", str(expectedResult), "--serialTime", str(serialTime), "--parallelTime", str(parallelTime), "--majorRad", str(majorR), "--minorRad", str(minorR)]
# print(" ".join(commandArray))
res = subprocess.call(commandArray)
print("Evaluation script exited with exit code : {}".format(res))

#Testcase 2---------------------------------------------------------------------------------------
print("\nExecuting Testcase2")
nPoints = 2000000007
nThreads = 4
majorR = 1.0
minorR = 1.0
expectedResult = nPoints
serialTime = 93
parallelTime = 26

print("Using {} threads".format(nThreads))
scratchFile = open(scratchFilePath, 'w')
commandArray = [execPath, "--nThreads", str(nThreads), "--nPoints", str(nPoints), "--majorRad", str(majorR), "--minorRad", str(minorR)]
print(" ".join(commandArray))
res = subprocess.call(commandArray, stdout=scratchFile)
scratchFile.close()
print("Test case 2 exited with exit code : {}".format(res))
print("Evaluating Test case 2...")
sys.stdout.flush()

commandArray = ["python", evaluationScriptPath, "--file", scratchFilePath, "--nThreads", str(nThreads), "--expectedResult", str(expectedResult), "--serialTime", str(serialTime), "--parallelTime", str(parallelTime), "--majorRad", str(majorR), "--minorRad", str(minorR)]
# print(" ".join(commandArray))
res = subprocess.call(commandArray)
print("Evaluation script exited with exit code : {}".format(res))



#Deleting scratchFile -----------------------------------------------------------------------------
commandArray = ["rm", scratchFilePath]
# print(" ".join(commandArray))
res = subprocess.call(commandArray)

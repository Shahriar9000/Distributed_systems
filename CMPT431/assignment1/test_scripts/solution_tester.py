#!/usr/bin/env python

import os
import re
import argparse
import subprocess
import time
import sys


uniqueString = "{}".format(int(time.time() * 1000000))
scratchFolder = "/tmp/scratch_"

parser = argparse.ArgumentParser()
parser.add_argument(
    '--execPath', help='Enter the full Path of the triangle_counting object file')
parser.add_argument('--scriptPath', help='Enter the full Path of the evaluation script', nargs='?',
                    default="/scratch/assignment0/test_scripts/solution_evalutor.pyc", const="")
parser.add_argument('--printOutput', help='Print scratch file', nargs='?', default="", const="0")

args = parser.parse_args()
execPath = args.execPath
evaluationScriptPath = args.scriptPath
scratchFilePath = scratchFolder+uniqueString
printOutput = args.printOutput

class TestCase(object):
    pass


testCase1 = TestCase()
testCase1.nProducers = 1
testCase1.nConsumers = 1
testCase1.nItems = 100000
testCase1.bufferSize = 50000

testCase2 = TestCase()
testCase2.nProducers = 4
testCase2.nConsumers = 4
testCase2.nItems = 1000000
testCase2.bufferSize = 8000000

testCase3 = TestCase()
testCase3.nProducers = 2
testCase3.nConsumers = 6
testCase3.nItems = 100000
testCase3.bufferSize = 10000

testCase4 = TestCase()
testCase4.nProducers = 5
testCase4.nConsumers = 3
testCase4.nItems = 100000
testCase4.bufferSize = 1000

testCases = [testCase1, testCase2, testCase3, testCase4]
# testCases = [testCase1]
curTestCase = 0

print("Evaluating " + execPath)
print("")

def runTests(testCase, testCaseNum):

    nProducers = testCase.nProducers
    nConsumers = testCase.nConsumers
    nItems = testCase.nItems
    bufferSize = testCase.bufferSize

    print("==========================================")
    print("Executing Testcase {}".format(testCaseNum))
    print("Using {} producers".format(nProducers))
    print("Using {} consumers".format(nConsumers))
    print("Using {} items".format(nItems))

    # Run executable and store output in scratchFile
    scratchFile = open(scratchFilePath, 'w')
    commandArray = [execPath, "--nProducers", str(nProducers), "--nConsumers", str(nConsumers),  "--nItems", str(nItems), "--bufferSize", str(bufferSize)]
    print(" ".join(commandArray))
    res = subprocess.call(commandArray, stdout=scratchFile)
    scratchFile.close()
    print("Test case 1 exited with exit code : {}".format(res))
    print("Evaluating Test case {}...".format(curTestCase))
    sys.stdout.flush()

    if printOutput:
        print("Reading scratchFile...")
        with open(scratchFilePath, 'r') as scratchFile:
            contents = scratchFile.read()
            print(contents)

    # Evaluate output scratchFile
    commandArray = ["python", evaluationScriptPath, "--file", scratchFilePath, "--nProducers", str(nProducers), "--nConsumers", str(nConsumers),  "--nItems", str(nItems)]
    # print(" ".join(commandArray))
    res = subprocess.call(commandArray)
    print("Evaluation script exited with exit code : {}".format(res))
    print("")


i = 0
for testCase in testCases:
    runTests(testCase, i+1)
    i = i + 1

    # Deleting scratchFile -----------------------------------------------------------------------------
commandArray = ["rm", scratchFilePath]
res = subprocess.call(commandArray)

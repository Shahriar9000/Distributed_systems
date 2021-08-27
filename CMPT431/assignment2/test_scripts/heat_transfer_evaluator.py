#!/usr/bin/env python

import csv
import os
import re
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--file', help='Enter the full Path of the output file', nargs='?', default="", const="")
parser.add_argument('--nThreads', help='Enter number of threads used', nargs='?', default=1, const=1)
parser.add_argument('--expectedResult', help='Enter the expected value')
parser.add_argument('--serialTime', help='Enter the baseline serial time taken')
parser.add_argument('--parallelTime', help='Enter the expected parallel time taken')

args = parser.parse_args()
inputFilePath = args.file
expectedResult = float(args.expectedResult)
nThreads = int(args.nThreads)
serialTime = float(args.serialTime)
parallelTime = float(args.parallelTime)

spacePattern = "(\s)*"
numberOfThreadsPattern = re.compile("^Number of threads{}:{}(\d+)".format(spacePattern, spacePattern))
heatTransferThreadStatsPattern = re.compile("^thread_id, start_column, end_column, time_taken")
resultPattern = re.compile("^Temp\[400,400\]\=(\d*\.\d*)")
timeTakenPattern = re.compile("^Time taken \(in seconds\){}:{}(\d*\.\d*)".format(spacePattern, spacePattern))


startProcessing = False
finishedProcessingThreadData = False
processingOverallData = False
numberOfColumns = 4

threadData = list()
nThreadsRead = 0
totalTime = 0.0
result = 0.0

consoleLogContents = ""
if(inputFilePath != ""):
    with open(inputFilePath, 'r') as inputFile:
        consoleLogContents = inputFile.read()

consoleLogContents = consoleLogContents.split('\n')

### Parse the output logs ###
for line in consoleLogContents:
    if(startProcessing == False):
        if(numberOfThreadsPattern.match(line)):
            lineSplit = line.split(':')
            lastWord = lineSplit[-1].strip()
            nThreadsRead = int(lastWord) 
            continue
        if(heatTransferThreadStatsPattern.match(line)):
            startProcessing = True
        continue

    # // Can proceed
    if(finishedProcessingThreadData == False):
        # print(line)
        lineSplit = line.split(',')                              
        if(len(lineSplit) == 4):
            threadID = lineSplit[0].strip()
            start_col = lineSplit[1].strip()
            end_col = lineSplit[2].strip()
            timeTaken = lineSplit[3].strip()
            threadData.append((threadID, int(start_col), int(end_col), float(timeTaken)))
            continue
        else:
            finishedProcessingThreadData = True

    # Process overall data
    if(resultPattern.match(line)):
        m = resultPattern.search(line)
        l = len(m.groups())
        result = float(m.group(l))
    if(timeTakenPattern.match(line)):
        m = timeTakenPattern.search(line)
        l = len(m.groups())
        totalTime = float(m.group(l))



### Validate all information is correctly parsed ###

# print("Parsed information from the program execution")
# print("Temp[400,400]={}".format(result))
# print("Time taken (in seconds) : {}".format(totalTime))
# print("Time taken (in seconds) : {}".format(result))

validationFlag = True
if nThreadsRead != nThreads:
    validationFlag = False
    print("VALIDATION FAILED: Incorrect number of threads used = {}".format(nThreadsRead))

if totalTime < 0.000000000000000001:
    validationFlag = False
    print("VALIDATION FAILED: Time taken = {}".format(totalTime))

if (result > (expectedResult+1)) or (result < (expectedResult-1)):
    validationFlag = False
    print("VALIDATION FAILED: Temp[400,400]({}) accuracy is very poor".format(result))

if nThreads != len(threadData):
    validationFlag = False
    print("VALIDATION FAILED: Thread statistics only found for {} threads".format(len(threadData)))

for value in threadData:
    if len(value) != 4:
        validationFlag = False
        print("VALIDATION FAILED: Incorrect format for thread statistics logs\n Expected format : {}".format(ellipseAreaThreadStatsPattern.pattern))

if validationFlag == True:
    print("Validation successful")
else:
    print("Validation failed")
    exit(1)



### Evaluation of the processed information ###

minInterval = 999999999999
maxInterval = 0
minTimeTaken = 99999999999.0
maxTimeTaken = 0.0
for v in threadData:
    startc = v[1]
    endc = v[2]
    timeTaken = v[3]
    minInterval = min(minInterval, endc-startc+1)
    maxInterval = max(maxInterval, endc-startc+1)
    minTimeTaken = min(minTimeTaken, timeTaken)
    maxTimeTaken = max(maxTimeTaken, timeTaken)


successFlag=True

#Validate work distribution
if(maxInterval-minInterval > 1):
    successFlag=False
    print("EVALUATION FAILED : Work is poorly distributed between threads.{}".format(expectedResult))
        
#Validate time taken with respect to baseline serial time
if(totalTime > serialTime):
    print("WARNING : Your parallel execution time ({}) is more than the serial execution time ({}).\nIdeally, your execution time should less than  {}".format(totalTime, serialTime, serialTime))
 
#Validate time taken with respect to expected parallel time
speedup = parallelTime / totalTime
# print("totalTime : {}".format(totalTime))
# print("serialTime : {}".format(serialTime))
# print("parallelTime : {}".format(parallelTime))
# print("speedup : {}".format(speedup))
slowdown = totalTime / parallelTime
print("Your parallel execution time, T : {} seconds".format(totalTime))
print("Expected parallel execution time, T_expected : {} seconds".format(parallelTime))
print("Ideally, T should be equal to (or less than) T_expected")

if(slowdown > 1.5):
    print("WARNING : Your parallel execution time ({} seconds) is more than 1.5 times higher than the expected parallel execution time ({} seconds).\nIdeally, your execution time should be around {} seconds".format(totalTime, parallelTime, parallelTime))


if successFlag == True:
    print("Evaluation successful")
else:
    print("Evaluation failed.")
    print("Ensure that you do not print more data than expected and the expected output format is strictly followed")


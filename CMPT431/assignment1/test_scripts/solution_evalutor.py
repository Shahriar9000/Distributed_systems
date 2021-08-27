
#!/usr/bin/env python

import csv
import os
import re
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--file', help='Enter the full Path of the output file',
                    nargs='?', default="", const="")
parser.add_argument(
    '--nProducers', help='Enter number of producer threads', nargs='?', default=1, const=1)
parser.add_argument(
    '--nConsumers', help='Enter number of consumer threads', nargs='?', default=1, const=1)
parser.add_argument('--nItems', help='Enter number of items',
                    nargs='?', default=1, const=1)

args = parser.parse_args()
inputFilePath = args.file
nProducers = int(args.nProducers)
nConsumers = int(args.nConsumers)
nItems = int(args.nItems)
expectedResult = nItems
expectedResultValue = (nItems * (nItems - 1)) / 2 #Since itemCount starts from 0, nItems-1 is the last element in the series

consumer_string = "^consumer_id, items_consumed, value_consumed, time_taken"
for p in range(nProducers):
    consumer_string = consumer_string + ", value_from_producer_%s" %p

spacePattern = "(\s)*"
numberOfItemsPattern = re.compile(
    "^Number of items:{}(\d+)".format(spacePattern, spacePattern))
numberOfProducersPattern = re.compile(
    "^Number of producers:{}(\d+)".format(spacePattern, spacePattern))
numberOfConsumersPattern = re.compile(
    "^Number of consumers:{}(\d+)".format(spacePattern, spacePattern))
queueSizePattern = re.compile(
    "^Queue size:{}(\d+)".format(spacePattern, spacePattern))
producerThreadStatsPattern = re.compile(
    "^producer_id, items_produced, value_produced, time_taken")
consumerThreadStatsPattern = re.compile(consumer_string)
#    "^consumer_id, items_consumed, value_consumed, time_taken")
totalProducedPattern = re.compile(
    "^Total produced{}={}(\d+)".format(spacePattern, spacePattern))
totalProducedValuePattern = re.compile(
    "^Total value produced{}={}(\d+)".format(spacePattern, spacePattern))
totalConsumedPattern = re.compile(
    "^Total consumed{}={}(\d+)".format(spacePattern, spacePattern))
totalConsumedValuePattern = re.compile(
    "^Total value consumed{}={}(\d+)".format(spacePattern, spacePattern))
timeTakenPattern = re.compile(
    "^Time taken \(in seconds\):{}(\d*\.\d*)".format(spacePattern, spacePattern))


startProcessingProducerData = False
startProcessingConsumerData = False
finishedProcessingProducerData = False
finishedProcessingConsumerData = False

processingOverallData = False
numberOfColumns = 4

producerData = list()
consumerData = list()

nProducersRead = 0
nConsumerssRead = 0

totalProduced = 0
totalProducedValue = 0
totalConsumed = 0
totalConsumedValue = 0
totalTime = 0.0

consoleLogContents = ""
if(inputFilePath != ""):
    with open(inputFilePath, 'r') as inputFile:
        consoleLogContents = inputFile.read()

consoleLogContents = consoleLogContents.split('\n')

### Parse the output logs ###
for line in consoleLogContents:
    if(startProcessingProducerData == False):
        if(numberOfProducersPattern.match(line)):
            lineSplit = line.split(':')
            lastWord = lineSplit[-1].strip()
            nProducersRead = int(lastWord)
            continue
        if(numberOfConsumersPattern.match(line)):
            lineSplit = line.split(':')
            lastWord = lineSplit[-1].strip()
            nConsumerssRead = int(lastWord)
            continue
        if(producerThreadStatsPattern.match(line)):
            startProcessingProducerData = True
        continue

    # // Can proceed
    if(finishedProcessingProducerData == False):
        lineSplit = line.split(',')
        if(len(lineSplit) == numberOfColumns):
            threadID = lineSplit[0].strip()
            itemsProduced = lineSplit[1].strip()
            valueProduced = lineSplit[2].strip()
            timeTaken = lineSplit[3].strip()
            producerData.append(
                (threadID, int(itemsProduced), int(valueProduced), float(timeTaken)))
            continue
        else:
            finishedProcessingProducerData = True

    if startProcessingConsumerData == False:
        if(totalProducedPattern.match(line)):
            lineSplit = line.split('=')
            lastWord = lineSplit[-1].strip()
            totalProduced = int(lastWord)
            continue
        if(totalProducedValuePattern.match(line)):
            lineSplit = line.split('=')
            lastWord = lineSplit[-1].strip()
            totalProducedValue = int(lastWord)
            continue
        if(consumerThreadStatsPattern.match(line)):
            startProcessingConsumerData = True
        continue

    if(finishedProcessingConsumerData == False):
        lineSplit = line.split(',')
        if(len(lineSplit) == (numberOfColumns+nProducers)):
            threadID = lineSplit[0].strip()
            itemsConsumed = lineSplit[1].strip()
            valueConsumed = lineSplit[2].strip()
            timeTaken = lineSplit[3].strip()
            consumerDataLine = [threadID, int(itemsConsumed), int(valueConsumed), float(timeTaken)]
            #append(
            #    (threadID, int(itemsConsumed), int(valueConsumed), float(timeTaken)))
            for p in range(nProducers):
                consumerDataLine.append(int(lineSplit[4+p].strip()))
            consumerData.append(consumerDataLine)
            continue
        else:
            finishedProcessingConsumerData = True

    # Process overall data
    if(totalConsumedPattern.match(line)):
        m = totalConsumedPattern.search(line)
        l = len(m.groups())
        totalConsumed = int(m.group(l))
    if(totalConsumedValuePattern.match(line)):
        m = totalConsumedValuePattern.search(line)
        l = len(m.groups())
        totalConsumedValue = int(m.group(l))
    if(timeTakenPattern.match(line)):
        m = timeTakenPattern.search(line)
        l = len(m.groups())
        totalTime = float(m.group(l))


### Validate all information is correctly parsed ###

# print("Parsed information from the program execution")
# print("Total points generated : {}".format(totalPoints))
# print("Total points in circle : {}".format(totalCirclePoints))
# print("Result : {}".format(result))
# print("Time taken (in seconds) : {}".format(totalTime))
# print("Time taken (in seconds) : {}".format(result))

validationFlag = True
if nProducersRead != nProducers:
    validationFlag = False
    print("VALIDATION FAILED: Incorrect number of producers used = {}".format(
        nProducersRead))

if nConsumerssRead != nConsumerssRead:
    validationFlag = False
    print("VALIDATION FAILED: Incorrect number of consumers used = {}".format(
        nConsumerssRead))

if totalProduced == 0:
    validationFlag = False
    print("VALIDATION FAILED: Total items produced = {}".format(totalProduced))

if totalProducedValue == 0:
    validationFlag = False
    print("VALIDATION FAILED: Total value of items produced = {}".format(totalProducedValue))

if totalConsumed == 0:
    validationFlag = False
    print("VALIDATION FAILED: Total items consumed = {}".format(totalConsumed))

if totalConsumedValue == 0:
    validationFlag = False
    print("VALIDATION FAILED: Total value of items consumed = {}".format(totalConsumedValue))

# if totalTime < 0.000000000000000001:
#     validationFlag = False
#     print("VALIDATION FAILED: Time taken = {}".format(totalTime))


if nProducers != len(producerData):
    validationFlag = False
    print("VALIDATION FAILED: Thread statistics only found for {} producers".format(
        len(producerData)))

if nConsumers != len(consumerData):
    validationFlag = False
    print("VALIDATION FAILED: Thread statistics only found for {} consumers".format(
        len(consumerData)))

for value in producerData:
    if len(value) != numberOfColumns:
        validationFlag = False
        print("VALIDATION FAILED: Incorrect format for producer statistics logs\n Expected format : {}".format(
            producerThreadStatsPattern.pattern))

for value in consumerData:
    if len(value) != (numberOfColumns+nProducers):
        validationFlag = False
        print("VALIDATION FAILED: Incorrect format for consumer statistics logs\n Expected format : {}".format(
            consumerThreadStatsPattern.pattern))

if validationFlag == True:
    print("Validation successful")
else:
    print("Validation failed")
    exit(1)


### Evaluation of the processed information ###

minItemsConsumed = 999999999999
maxItemsConsumed = 0
minTimeTaken = 99999999999.0
maxTimeTaken = 0.0
totalItemsConsumedByThreads = 0
totalValueOfItemsConsumedByThreads = 0
value_by_producer=[]
for p in range(nProducers):
    value_by_producer.append(0)
for v in consumerData:
    itemsConsumed = v[1]
    valueConsumed = v[2]
    timeTaken = v[3]
    for p in range(nProducers):
        value_by_producer[p] += v[4+p]
    minItemsConsumed = min(minItemsConsumed, itemsConsumed)
    maxItemsConsumed = max(maxItemsConsumed, itemsConsumed)
    minTimeTaken = min(minTimeTaken, timeTaken)
    maxTimeTaken = max(maxTimeTaken, timeTaken)
    totalItemsConsumedByThreads += itemsConsumed
    totalValueOfItemsConsumedByThreads += valueConsumed

minItemsProduced = 999999999999
maxItemsProduced = 0
minTimeTaken = 99999999999.0
maxTimeTaken = 0.0
totalItemsProducedByThreads = 0
totalValueOfItemsProducedByThreads = 0
value_by_producer_match = True

p = 0
for v in producerData:
    itemsProduced = v[1]
    valueProduced = v[2]
    timeTaken = v[3]
    if (value_by_producer[p] != valueProduced):
        value_by_producer_match = False
    p  = p+1
    minItemsProduced = min(minItemsProduced, itemsProduced)
    maxItemsProduced = max(maxItemsProduced, itemsProduced)
    minTimeTaken = min(minTimeTaken, timeTaken)
    maxTimeTaken = max(maxTimeTaken, timeTaken)
    totalItemsProducedByThreads += itemsProduced
    totalValueOfItemsProducedByThreads += valueProduced


successFlag = True

# Validate number of items produced
if(totalItemsProducedByThreads != expectedResult):
    successFlag = False
    print("EVALUATION FAILED : Total number of items produced is incorrect({}). Expected {}.".format(
        totalItemsProducedByThreads, expectedResult))

if(totalValueOfItemsProducedByThreads != expectedResultValue):
    successFlag = False
    print("EVALUATION FAILED : Total value of items produced is incorrect({}). Expected {}.".format(
        totalValueOfItemsProducedByThreads, expectedResultValue))

if (value_by_producer_match == False):
    successFlag = False
    print("EVALUATION FAILED : Total value produced by (at least) one producer doesn't match total consumers' value for theat producer")

# Validate number of items consumed
if(totalItemsConsumedByThreads != expectedResult):
    successFlag = False
    print("EVALUATION FAILED : Total number of items consumed is incorrect({}). Expected {}.".format(
        totalItemsConsumedByThreads, expectedResult))

if(totalValueOfItemsConsumedByThreads != expectedResultValue):
    successFlag = False
    print("EVALUATION FAILED : Total value of items consumed is incorrect({}). Expected {}.".format(
        totalValueOfItemsConsumedByThreads, expectedResultValue))


if successFlag == True:
    print("Evaluation successful")
else:
    print("Evaluation failed.")
    print("Ensure that you do not print more data than expected and the expected output format is strictly followed")

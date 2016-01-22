#!/bin/python
from __future__ import print_function
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from operator import itemgetter
from os import listdir
from os.path import isfile, join

latencyLogger = "fi.aalto.dmg.statistics.Latency"

# "HH:mm:ss,SSS" to milliseconds
def to_milliseconds(timeStr):
    tmpList = timeStr.split(',')
    tmpList2 = tmpList[0].split(':')
    milliseconds = long(tmpList[1])
    seconds = long(tmpList2[0])*3600+long(tmpList2[1])*60+long(tmpList2[2])
    return milliseconds+seconds*1000

# extract throughput log from flink log file
def extract_latency_log(infileName, outfileName):
    with open(infileName, 'r') as infile, open(outfileName, 'w+') as outfile:
        for line in infile:
            if latencyLogger in line:
                time = to_milliseconds(line[:12])
                latency = long(line.split('\t')[1])
                outfile.write(str(time) + "\t" + str(latency) + "\n")     

# concatenate files into one single file
def concatenate(outfileName, infileNames):
    with open(outfileName, 'w') as outfile:
        for fname in infileNames:
            with open(fname) as infile:
                outfile.write(infile.read())

# analysis latency              
def analysis(infileName):
    latencyArray = []
    with open(infileName, 'r') as infile:
        for line in infile:
            tmpList = line.split('\t')
            latencyArray.append({'time': long(tmpList[0]),
                                    'latency': long(tmpList[1])})
    # generate pandas DataFrame    
    latencyArray = sorted(latencyArray, key=itemgetter('latency'))    
    df = pd.DataFrame(latencyArray)
    
    total_count = df['latency'].count()
    
    percentageList = [0.7, 0.8, 0.9]
    latencyList = []
    for p in percentageList:
        index = long(p*total_count)-1
        latencyList.append(df['latency'][index])
    print(latencyList)

def process(inFolderName):
    onlyfiles = [join(inFolderName, f) for f in listdir(inFolderName) if isfile(join(inFolderName, f))]
    print(onlyfiles)
    with open(inFolderName+'.log', 'w') as outfile:
        for fname in onlyfiles:
            with open(fname) as infile:
                for line in infile:
                    if latencyLogger in line:
                        time = to_milliseconds(line[:12])
                        latency = long(line.split('\t')[1])
                        outfile.write(str(time) + "\t" + str(latency) + "\n") 
    analysis(inFolderName+'.log')            
                    
    
#     
if __name__ == "__main__":
    commands = ["extract", "combine", "analysis", "process"]
    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print("Usage: python %s command" % sys.argv[0])
        print("commands includes: ", commands)
        
    if "extract" == sys.argv[1]:
        if len(sys.argv) < 4:
            print("Usage: python %s extract infile outfile" % sys.argv[0])
            exit(1)
        else:
            print("Start extract ...")
            extract_latency_log(sys.argv[2], sys.argv[3])
    if "combine" == sys.argv[1]:
        if len(sys.argv) < 4:
            print("Usage: python %s combine outfile infile1, infile2 ..." % sys.argv[0])
        else:
            concatenate(sys.argv[2], sys.argv[3:])
    if "analysis" == sys.argv[1]:
        if len(sys.argv) < 3:
            print("Usage: python %s analysis infile" % sys.argv[0])
        else:
            analysis(sys.argv[2])
    if "process" == sys.argv[1]:
        if len(sys.argv) < 3:
            print("Usage: python %s process infolder" % sys.argv[0])
        else:
            process(sys.argv[2])
    exit(0)


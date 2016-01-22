#!/bin/python
from __future__ import print_function
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from os import listdir
from os.path import isfile, join

throughputLogger = "fi.aalto.dmg.statistics.Throughput"

# "HH:mm:ss,SSS" to milliseconds
def to_milliseconds(timeStr):
    tmpList = timeStr.split(',')
    tmpList2 = tmpList[0].split(':')
    milliseconds = long(tmpList[1])
    seconds = long(tmpList2[0])*3600+long(tmpList2[1])*60+long(tmpList2[2])
    return milliseconds+seconds*1000

# extract throughput log from flink log file
def extract_throughput_log(infileName, outfileName):
    with open(infileName, 'r') as infile, open(outfileName, 'w+') as outfile:
        for line in infile:
            if throughputLogger in line:
                time = to_milliseconds(line[:12])
                period = line.split('\t')[1]
                elements = line.split('\t')[2]
                outfile.write(str(time) + "\t" + period + "\t" + elements + "\n")     

# concatenate files into one single file
def concatenate(outfileName, infileNames):
    with open(outfileName, 'w') as outfile:
        for fname in infileNames:
            with open(fname) as infile:
                outfile.write(infile.read())

# analysis throughput, assume that interval is much larger than 'period'               
def analysis(infileName, outfileName, interval):
    throughputArray = []
    with open(infileName, 'r') as infile:
        for line in infile:
            tmpList = line.split('\t')
            end = long(tmpList[0])
            if end < 46369188:
                end += 86400000
            throughputArray.append({'end': end,
                                    'period': long(tmpList[1]),
                                    'counts': long(tmpList[2])})
    # generate pandas DataFrame        
    df = pd.DataFrame(throughputArray)
    df['start'] = df['end'] - df['period']
    df = df.sort_values(by=['start'])
    df.sort_values('start')
    
    minTime = df['start'].min()    
    periodStart = minTime
    intervalThroughput = []       
    period = {'start': periodStart, 'counts': 0}
    nextPeriod = {'start': periodStart+interval, 'counts': 0}
    
    for index, row in df.iterrows():
        while row['start'] >= periodStart+interval:
            intervalThroughput.append(period)
            period = nextPeriod
            periodStart += interval
            nextPeriod = {'start': periodStart+interval, 'counts': 0}
            
        if row['end'] < periodStart + interval:
            period['counts'] += row['counts']
        else:
            # whether it is still in 10s
            countsInFirstInterval = row['counts']*(periodStart + interval - row['start'])/row['period']
            period['counts'] += countsInFirstInterval
            nextPeriod['counts'] += row['counts'] - countsInFirstInterval
            if row['start'] > periodStart + interval:
                print(row)
            if row['counts'] - countsInFirstInterval < 0:
                print('row["counts"] - countsInFirstInterval < 0')
    intervalThroughput.append(period)   
    intervalThroughput.append(nextPeriod)
    
    # generat result DataFrame
    intervalDf = pd.DataFrame(intervalThroughput)
    # fommat time to seconds and start from 0s
    intervalDf['start'] -= intervalDf['start'].min()
    intervalDf['start'] /= 1000
    intervalDf.to_csv(outfileName, sep='\t')
    # plot
    plt.style.use('ggplot')
    intervalDf.plot(x='start', y='counts', kind='line', title='Cluster Throughput each 10 seconds')
    plt.show()
    
def process(inFolderName, outfileName, interval):
    onlyfiles = [join(inFolderName, f) for f in listdir(inFolderName) if isfile(join(inFolderName, f))]
    print(onlyfiles)
    with open(inFolderName+'.log', 'w') as outfile:
        for fname in onlyfiles:
            with open(fname) as infile:
                for line in infile:
                    if throughputLogger in line:
                        time = to_milliseconds(line[:12])
                        period = line.split('\t')[1]
                        elements = line.split('\t')[2]
                        outfile.write(str(time) + "\t" + period + "\t" + elements + "\n") 
    analysis(inFolderName+'.log', outfileName, interval)            
                    
#     
if __name__ == "__main__":
    commands = ["extract", "combine", "analysis", "process"]
    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print("Usage: python %s command" % sys.argv[0])
        print("commands includes: ", commands)
        print("analysis is used to analysis extracted and combined log file")
        print("process is used to origin log files under one folder")
        exit(1)
    if "extract" == sys.argv[1]:
        if len(sys.argv) < 4:
            print("Usage: python %s extract infile outfile" % sys.argv[0])
            exit(1)
        else:
            print("Start extract ...")
            extract_throughput_log(sys.argv[2], sys.argv[3])
    if "combine" == sys.argv[1]:
        if len(sys.argv) < 4:
            print("Usage: python %s combine outfile infile1, infile2 ..." % sys.argv[0])
        else:
            concatenate(sys.argv[2], sys.argv[3:])
    if "analysis" == sys.argv[1]:
        if len(sys.argv) < 4:
            print("Usage: python %s analysis infile outfile" % sys.argv[0])
        else:
            # default interval 10 seconds
            interval = 10000
            analysis(sys.argv[2], sys.argv[3], interval)
    if "process" == sys.argv[1]:
        if len(sys.argv) < 4:
            print("Usage: python %s process infolder outfile" % sys.argv[0])
        else:
            # default interval 10 seconds
            interval = 10000
            process(sys.argv[2], sys.argv[3], interval)
    exit(0)


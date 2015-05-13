#!/bin/bash

hadoop fs -mkdir rfinput
hadoop fs -mkdir rfoutput

hadoop fs -put src/main/resources/* rfinput

hadoop jar target/WordCountHadoopProject-0.0.1-SNAPSHOT.jar com.hadoop.relativefrequency.hybrid.RelativeFrequencyTool -D mapred.reduce.tasks=1 rfinput/*.txt rfoutput

hadoop fs -get rfoutput/* output/hybrid

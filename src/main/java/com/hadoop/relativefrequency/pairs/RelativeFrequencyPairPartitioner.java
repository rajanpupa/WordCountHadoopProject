package com.hadoop.relativefrequency.pairs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RelativeFrequencyPairPartitioner extends Partitioner<WordPairKeyObject, DoubleWritable>{

	@Override
	public int getPartition(WordPairKeyObject arg0, DoubleWritable arg1, int arg2) {
		return arg0.hashCode() % arg2;
		//return 0;
	}

}

package com.hadoop.relativefrequency.pairs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RelativeFrequencyPairPartitioner extends Partitioner<WordPairKeyObject, DoubleWritable>{

	@Override
	public int getPartition(WordPairKeyObject key, DoubleWritable value, int numReducers) {
		return key.getWord().hashCode() % numReducers;
		//return 0;
	}

}

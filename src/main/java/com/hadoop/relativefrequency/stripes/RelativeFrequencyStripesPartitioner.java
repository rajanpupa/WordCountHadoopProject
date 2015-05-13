package com.hadoop.relativefrequency.stripes;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RelativeFrequencyStripesPartitioner extends Partitioner<Text, MapWritable>{

	@Override
	public int getPartition(Text key, MapWritable value, int numReducers) {
		return key.hashCode() % numReducers;
		//return 0;
	}

}

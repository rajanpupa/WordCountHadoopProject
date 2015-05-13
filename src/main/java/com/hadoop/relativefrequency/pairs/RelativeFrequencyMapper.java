package com.hadoop.relativefrequency.pairs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelativeFrequencyMapper extends
		Mapper<Object, Text, WordPairKeyObject, IntWritable> {

	private final IntWritable ONE = new IntWritable(1);
	WordPairKeyObject pair = new WordPairKeyObject();
	String dummyWord = "*";
	private String REGEX_WHITEWORDS = "([\\s])+";

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] csv = value.toString().split(REGEX_WHITEWORDS);

		String mainWord;
		String neighborWord;

		for (int i = 0; i < csv.length; i++) {
			mainWord = csv[i];

			for (int j = i + 1; j < csv.length; j++) {

				neighborWord = csv[j];

				if (!mainWord.equals(neighborWord)) {
					emitCustom(mainWord, neighborWord, context);
				}
			}
		}
	}

	public void emitCustom(String first, String second, Context context)
			throws IOException, InterruptedException {
		//pair.setWord(first);
		//pair.setNeighbor(second);

		context.write(new WordPairKeyObject(new Text(first), new Text(second)), ONE);
		
		//pair.setNeighbor(dummyWord);
		context.write(new WordPairKeyObject(new Text(first), new Text(dummyWord)), ONE);
	}
}

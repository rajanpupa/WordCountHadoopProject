package com.hadoop.relativefrequency.hybrid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelativeFrequencyInMapperCombiner extends
		Mapper<Object, Text, WordPairKeyObject, DoubleWritable> {

	private final DoubleWritable ONE = new DoubleWritable(1d);
	WordPairKeyObject pair = new WordPairKeyObject();
	String dummyWord = "*";
	private String REGEX_WHITEWORDS = "([\\s])+";
	
	Map<WordPairKeyObject, Double> map = new HashMap<WordPairKeyObject, Double>();

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
					//emitCustom(mainWord, neighborWord, context);
					//put in hashmap
					mapCustom(mainWord, neighborWord, context);
					//mapCustom(mainWord, dummyWord, context);
				}
			}
		}
	}
	
	public void mapCustom(String first, String second, Context context)
			throws IOException, InterruptedException {
		WordPairKeyObject key = new WordPairKeyObject(new Text(first), new Text(second));
		WordPairKeyObject dummy = new WordPairKeyObject(new Text(first), new Text(dummyWord));
		
		if(map.containsKey(key)){
			map.put(key, map.get(key) + 1);
		}else{
			map.put(key, 1d);
		}
	}

	@Override
	protected void cleanup(
			Mapper<Object, Text, WordPairKeyObject, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		for(WordPairKeyObject key: map.keySet()){
			context.write(key, new DoubleWritable(map.get(key)));
		}
	}
	
	
}

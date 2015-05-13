package com.hadoop.relativefrequency.stripes;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelativeFrequencyMapper extends
		Mapper<Object, Text, Text, MapWritable> {

	private final DoubleWritable ONE = new DoubleWritable(1d);
	private String REGEX_WHITEWORDS = "([\\s])+";
	
	MapWritable map = new MapWritable();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] csv = value.toString().split(REGEX_WHITEWORDS);

		String mainWord;
		String neighborWord;
		
		
		
		for (int i = 0; i < csv.length; i++) {
			mainWord = csv[i];
			map = new MapWritable();
			for (int j = i + 1; j < csv.length; j++) {
				neighborWord = csv[j];
				if (!mainWord.equals(neighborWord)) {
					mapput(neighborWord);
				}else{
					break;
				}
			}
			emitCustom(mainWord, map, context);
		}
		
	}
	
	public void mapput(String neighborWord){
		Text neighborToken = new Text(neighborWord);
		
		if(map.containsKey(neighborToken)){
			DoubleWritable count = (DoubleWritable) map.get(neighborToken);
			count.set(count.get() + 1);
		}else{
			map.put(neighborToken, ONE);
		}
		
	}

	public void emitCustom(String first, MapWritable map, Context context)
			throws IOException, InterruptedException {
		
		context.write(new Text(first), map);
	}

	
	
}

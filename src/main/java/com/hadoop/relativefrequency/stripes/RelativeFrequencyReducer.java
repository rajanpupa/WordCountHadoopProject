package com.hadoop.relativefrequency.stripes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends
		Reducer<Text, MapWritable, Text, Text> {
	
	DoubleWritable dw = new DoubleWritable();
	
	String word;
	String neighbor;
	
	Double totalCount = 0d;
	
	//MapWritable map = new MapWritable();
	Map<String, Double> map = new HashMap<String, Double>();
	
	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
		
		totalCount = 0d;
		 //map = new MapWritable();
		map = new HashMap<String, Double>();
		for(MapWritable value : values){
			for(Entry<Writable, Writable> entry : value.entrySet()){
				if( map.containsKey( entry.getKey().toString() ) ){
					Double count = (Double) map.get(entry.getKey().toString() );
					count= count + Double.parseDouble(entry.getValue().toString()) ;
				}else{
					map.put(entry.getKey().toString(), ((DoubleWritable)entry.getValue()).get());
				}
				totalCount += Double.parseDouble(entry.getValue().toString());
				
			}
		}
		
		
		for(String k: map.keySet()){
			map.put(k, map.get(k)/totalCount);
		}
		
		context.write(key, new Text(map.toString()) );
		
    }
	
}

package com.hadoop.relativefrequency.stripes;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends
		Reducer<Text, MapWritable, Text, DoubleWritable> {
	
	DoubleWritable dw = new DoubleWritable();
	
	String word;
	String neighbor;
	
	double totalCount = 0;
	
	MapWritable map = new MapWritable();
	
	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
		
		totalCount = 0;
		 map = new MapWritable();
		for(MapWritable value : values){
			for(Entry<Writable, Writable> entry : value.entrySet()){
				if( map.containsKey( entry.getKey() ) ){
					DoubleWritable count = (DoubleWritable) map.get(entry.getKey());
					count.set( count.get() + Double.parseDouble(entry.getValue().toString()) );
				}else{
					map.put(entry.getKey(), entry.getValue());
				}
				totalCount += Double.parseDouble(entry.getValue().toString());
				
				//context.write(new Text("["+key.toString()+","+entry.getKey().toString()+"]"), (DoubleWritable) entry.getValue());
			}
		}
		
		for(Entry<Writable, Writable> entry : map.entrySet()){
			double occurence = Double.parseDouble(entry.getValue().toString());
			context.write(new Text("["+key.toString()+","+entry.getKey().toString()+"]"), new DoubleWritable(occurence/totalCount));
		}
		
		//test
//		for(MapWritable mw: values){
//			context.write(key, new DoubleWritable(totalCount++) );
////			for(Writable n: mw.keySet()){
////				double occurence = Double.parseDouble(mw.get(n).toString());
////				context.write(new Text("["+key.toString()+","+mw.get(n).toString()+"]"), new DoubleWritable(totalCount++));
////			}
//		}
		//map.clear();
    }
	
}

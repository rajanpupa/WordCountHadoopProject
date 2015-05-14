package com.hadoop.relativefrequency.hybrid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends
		Reducer<WordPairKeyObject, DoubleWritable, Text, Text> {
	
	DoubleWritable dw = new DoubleWritable();
	
	String word=null;
	String neighbor=null;
	
	double currentCount = 0;
	double totalCount = 0;
	
	Map<String, Double> map= new HashMap<String, Double>();
	
	@Override
	public void reduce(WordPairKeyObject pair, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
		
		currentCount = getSum(values);
		
        if(pair.getWord().toString().equals(word)){//old word
        	//emitCustom(pair, currentCount/totalCount, context);
        	map.put(pair.getNeighbor().toString(), currentCount);
        }else{// word and neighbor changed *
        	if(word!=null){
        		//emit all
        		emitAll(context);
        		word = pair.getWord().toString();
        		//renew map
        	}else{
        		//don't emit here
        		word = pair.getWord().toString();
        		//map.put(pair.getNeighbor().toString(), currentCount);
            	//emitCustom(pair, currentCount/totalCount, context);
        	}
        	map.put(pair.getNeighbor().toString(), currentCount);
        	
        }
    }
	
	
	
	@Override
	protected void cleanup(
			Reducer<WordPairKeyObject, DoubleWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		emitAll(context);
	}



	public void emitAll(Context context) throws IOException, InterruptedException{
		totalCount = 0;
		for(String str: map.keySet()){
			totalCount+= map.get(str);
		}
		
//		for(String str: map.keySet()){
//			context.write(new Text("[" + word + ", " + str + "]"), new DoubleWritable(map.get(str)/totalCount));
//		}
		for(String k: map.keySet()){
			map.put(k, map.get(k)/totalCount);
		}
		context.write(new Text(word), new Text(map.toString() ));
		map = new HashMap<String, Double>();
	}
	
	public double getSum(Iterable<DoubleWritable> values){
		double sum = 0;
		DoubleWritable iw ;
		java.util.Iterator<DoubleWritable> it = values.iterator();
		
		while(it.hasNext()){
			iw = it.next();
			sum += iw.get();
		}
		
		return sum>0?sum:200;
	}
	
//	public void emitCustom(WordPairKeyObject key, double value, Context context) 
//			throws IOException, InterruptedException{
//		//dw.set(value);
//		context.write(key.toText(), new DoubleWritable(value));
//	}

//	public void emitCustom(Text word, Text neighbor, double value, Context context) 
//			throws IOException, InterruptedException{
//		//dw.set(value);
//		context.write(new WordPairKeyObject(word, neighbor).toText()
//				, new DoubleWritable(value));
//	}
	
	public static void main(String[] args) {
		WordPairKeyObject wp1=new WordPairKeyObject("123", "*");
		WordPairKeyObject wp2=new WordPairKeyObject("123", "1");
		
		if(wp1.getWord().equals(wp2.getWord())){//sameword
			if(wp1.getNeighbor().equals(wp2.getNeighbor())){//same neighbor
				System.out.println("same neighbor");
			}else{
				System.out.println("different neighbor");
			}
		}
	}

}

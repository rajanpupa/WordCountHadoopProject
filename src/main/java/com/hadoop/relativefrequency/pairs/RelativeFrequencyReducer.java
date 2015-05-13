package com.hadoop.relativefrequency.pairs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends
		Reducer<WordPairKeyObject, IntWritable, Text, IntWritable> {
	
	DoubleWritable dw = new DoubleWritable();
	
	String word;
	String neighbor;
	
	Integer currentCount = 0;
	Integer totalCount = 0;
	
	@Override
	public void reduce(WordPairKeyObject pair, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
		
		currentCount = getSum(values);
        
        if(pair.getWord().toString().equals(word)){//old word
        	emitCustom(pair, currentCount, context);
        }else{// word and neighbor changed
        	word = pair.getWord().toString();
        	neighbor = pair.getNeighbor().toString();
        	totalCount = currentCount;
        }
    }
	
	public int getSum(Iterable<IntWritable> values){
		int sum = 0;
		IntWritable iw ;
		java.util.Iterator<IntWritable> it = values.iterator();
		
		while(it.hasNext()){
			iw = it.next();
			sum += iw.get();
		}
		
		return sum>0?sum:200;
	}
	
	public void emitCustom(WordPairKeyObject key, Integer value, Context context) 
			throws IOException, InterruptedException{
		//dw.set(value);
		context.write(key.toText(), new IntWritable(value));
	}

	public void emitCustom(Text word, Text neighbor, Integer value, Context context) 
			throws IOException, InterruptedException{
		//dw.set(value);
		context.write(new WordPairKeyObject(word, neighbor).toText()
				, new IntWritable(value));
	}
	
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

package com.hadoop.relativefrequency.pairs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducerEr extends
		Reducer<WordPairKeyObject, DoubleWritable, Text, DoubleWritable> {
	
	DoubleWritable dw = new DoubleWritable();
	/*
	 * DONT USE THE TEXT TO STORE THE STATE, THE TEXT IS IMMUTABLE INSTANCE, AND THE EQUALS METHOD DOES NOT WORK PROPERLY.
	 * use the string instead. text.toString()
	 */
	Text word;
	Text neighbor;
	
	double currentCount = 0;
	double totalCount = 0;
	
	@Override
	public void reduce(WordPairKeyObject pair, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
		
		currentCount = getSum(values);
        
        if(pair.getWord().equals(word)){//old word
        	emitCustom(pair, currentCount/totalCount, context);
        }else{// word and neighbor changed *
        	word = pair.getWord();
        	neighbor = pair.getNeighbor();
        	totalCount = currentCount;
        	//emitCustom(pair, currentCount/totalCount, context);
        }
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
	
	public void emitCustom(WordPairKeyObject key, double value, Context context) 
			throws IOException, InterruptedException{
		//dw.set(value);
		context.write(key.toText(), new DoubleWritable(value));
	}

	public void emitCustom(Text word, Text neighbor, double value, Context context) 
			throws IOException, InterruptedException{
		//dw.set(value);
		context.write(new WordPairKeyObject(word, neighbor).toText()
				, new DoubleWritable(value));
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

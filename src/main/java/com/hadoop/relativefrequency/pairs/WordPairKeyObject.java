package com.hadoop.relativefrequency.pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordPairKeyObject implements WritableComparable<WordPairKeyObject>{
	private Text word;
	private Text neighbor;
	
	public WordPairKeyObject(){
		word = new Text();
		neighbor = new Text();
	}
	
	public WordPairKeyObject(Text first, Text second){
		word = first==null?new Text():first;
		neighbor = second==null?new Text():second;
	}
	
	public WordPairKeyObject(String first, String second){
		word = first==null?new Text():new Text(first);
		neighbor = second==null?new Text():new Text(second);
	}

	// Getters and Setters
	public Text getWord() {
		return word;
	}

	public void setWord(String firstItem) {
		this.word.set(firstItem);
	}

	public Text getNeighbor() {
		return neighbor;
	}

	public void setNeighbor(String secondItem) {
		this.neighbor.set(secondItem);
	}

	@Override
	public int compareTo(WordPairKeyObject o) {
		if(! this.word.equals(o.word)){
			return this.word.compareTo(o.word);
		}else{
			return this.neighbor.compareTo(o.neighbor);
		}
	}
	
	static class WordPairComparator implements Comparator<WordPairKeyObject>{

		@Override
		public int compare(WordPairKeyObject o1, WordPairKeyObject o2) {
			return o1.compareTo(o2);
		}
	}
	
	public static Comparator<WordPairKeyObject> getComparator(){
		return new WordPairComparator();
	}
	
	public String toString(){
		return "( " + this.word + ", " + this.neighbor + ")";
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		if(word == null){
			word = new Text();
		}
		if(neighbor == null){
			neighbor = new Text();
		}
		
		word.readFields(arg0);
		neighbor.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		word.write(arg0);
		neighbor.write(arg0);
	}
	
	public int hashCode(){
		return Math.abs(word.hashCode()*31 + neighbor.hashCode());
	}
	
	public Text toText(){
		Text t = new Text("["+word.toString()+","+neighbor.toString()+"]");
		return t;
	}
	public boolean equals(WordPairKeyObject arg0){
		return this.word.equals(arg0.getWord()) && this.neighbor.equals(arg0.getNeighbor());
	}
	
	public static void main(String[] args) {
		List<WordPairKeyObject> abc = new ArrayList<WordPairKeyObject>();
		abc.add(new WordPairKeyObject("12", "23"));
		abc.add(new WordPairKeyObject("12", "*"));
		abc.add(new WordPairKeyObject("12", "11"));
		abc.add(new WordPairKeyObject("23", "23"));
		abc.add(new WordPairKeyObject("13", "*"));
		abc.add(new WordPairKeyObject("15", "14"));
		abc.add(new WordPairKeyObject("15", "23"));
		
		Collections.sort(abc);
		
		for(WordPairKeyObject wp: abc){
			System.out.println(wp);
		}
	}
}

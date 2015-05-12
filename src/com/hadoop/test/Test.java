package com.hadoop.test;

public class Test {

	public static void main(String[] args) {
		String REGEX_SPACE_TABS = "([ 	]){1,}";
		String REGEX_WHITEWORDS = "([\\s])+";
		
		
		String line = "My name is  Rajan Prasad	Upadhyay";
		String[] words = line.split( REGEX_WHITEWORDS );
		
		for(String word: words){
			System.out.println(word);
		}
	}
}

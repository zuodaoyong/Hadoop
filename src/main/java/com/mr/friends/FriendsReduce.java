package com.mr.friends;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendsReduce extends Reducer<Text,Text,Text,NullWritable>{

	Set<Character> set=new HashSet<>(); 
	@Override
	protected void reduce(Text text, Iterable<Text> iterable,Context context)
			throws IOException, InterruptedException {
		set.clear();
		for(Text str:iterable){
			String string = str.toString();
			if(string.charAt(0)!=text.toString().charAt(0)){
				set.add(string.charAt(0));
			}
			if(string.charAt(1)!=text.toString().charAt(0)){
				set.add(string.charAt(1));
			}
		}
		
		context.write(new Text(text+"->"+set.toString()), NullWritable.get());
	}
}

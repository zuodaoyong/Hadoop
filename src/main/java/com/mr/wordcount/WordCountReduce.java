package com.mr.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{


	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	    Iterator<IntWritable> iterator = iterable.iterator();
		int sum=0;
	    while(iterator.hasNext()){
			IntWritable intWritable=iterator.next();
			sum+=intWritable.get();
		}
	    context.write(text, new IntWritable(sum));
	}
}

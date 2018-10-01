package com.mr.wordcount;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCombiner extends Reducer<Text,IntWritable, Text,IntWritable>{

	int sum=0;
	@Override
	protected void reduce(Text text, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		Iterator<IntWritable> iterator = values.iterator();
		sum=0;
		while(iterator.hasNext()){
			sum+=iterator.next().get();
		}
		context.write(text,new IntWritable(sum));
	}
}

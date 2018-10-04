package com.mr.index2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneIndexReduce extends Reducer<Text,IntWritable,Text,IntWritable>{

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,
			Context context) throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable intWritable:iterable){
			sum+=intWritable.get();
		}
		context.write(text, new IntWritable(sum));
	}
}

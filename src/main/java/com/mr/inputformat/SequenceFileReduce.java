package com.mr.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SequenceFileReduce extends Reducer<Text,BytesWritable, Text,BytesWritable>{

	@Override
	protected void reduce(Text text, Iterable<BytesWritable> iterable,
			Context context) throws IOException, InterruptedException {
		for(BytesWritable writable:iterable){
			context.write(text, writable);
		}
	}
}

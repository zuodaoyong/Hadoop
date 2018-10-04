package com.mr.index2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OneIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		FileSplit fileSplit=(FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		String[] splits = line.split("\\s");
		for(String str:splits){
			context.write(new Text(str+"-"+fileName), new IntWritable(1));
		}
	}
}

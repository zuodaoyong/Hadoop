package com.mr.wordcount;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		if(StringUtils.isNotEmpty(line)){
			String[] splits = line.split("，");
			for(String str:splits){
				context.write(new Text(str),new IntWritable(1));
			}
		}
	}
}

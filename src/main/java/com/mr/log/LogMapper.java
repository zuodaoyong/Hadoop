package com.mr.log;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
	String[] splits=null;
	Text k=new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//获取一行
		String line = value.toString();
		//解析log
		boolean result=parseLog(line,context);
		if(!result){
			return;
		}
		k.set(line);
		context.write(k, NullWritable.get());
	}

	private boolean parseLog(String line,Context context) {
		splits = line.split("\\s");
		if(splits.length>11){
			context.getCounter("logMapper","parseLog_true").increment(1);
			return true;
		}
		context.getCounter("logMapper", "parseLog_false").increment(1);
		return false;
	}
}

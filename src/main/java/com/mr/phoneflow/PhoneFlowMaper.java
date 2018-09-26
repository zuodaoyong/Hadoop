package com.mr.phoneflow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PhoneFlowMaper extends Mapper<LongWritable, Text, Text,PhoneFlow>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PhoneFlow>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] splits = line.split(" ");
		String phone=splits[1];
		Long upFlow=Long.valueOf(splits[splits.length-2]);
		Long lowFlow=Long.valueOf(splits[splits.length-3]);
		PhoneFlow phoneFlow=new PhoneFlow();
		phoneFlow.setPhone(phone);
		phoneFlow.setLowFlow(lowFlow);
		phoneFlow.setUpFlow(upFlow);
		phoneFlow.setTotalFlow(upFlow+lowFlow);
		context.write(new Text(phone),phoneFlow);
	}
}

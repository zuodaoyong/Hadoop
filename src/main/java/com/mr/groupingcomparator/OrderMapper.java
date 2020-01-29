package com.mr.groupingcomparator;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text,Order,NullWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Order, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] splits = line.split("\t");
		Order order=new Order();
		order.setId(splits[0]);
		order.setPrice(Float.valueOf(splits[2]));
		context.write(order, NullWritable.get());
	}
}

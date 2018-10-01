package com.mr.groupingcomparator;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReduce extends Reducer<Order,NullWritable,Order,NullWritable>{

	@Override
	protected void reduce(Order order, Iterable<NullWritable> iterable,
			Context context) throws IOException, InterruptedException {
		System.out.println("order="+order.toString());
		context.write(order, NullWritable.get());
	}
}

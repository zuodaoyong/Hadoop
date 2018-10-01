package com.mr.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartition extends Partitioner<Order,NullWritable>{

	@Override
	public int getPartition(Order order, NullWritable nullWritable, int numReduces) {
		return (order.getId().hashCode()&Integer.MAX_VALUE)%numReduces;
	}

}

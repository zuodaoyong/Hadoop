package com.mr.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingCompartor extends WritableComparator{

	public OrderGroupingCompartor(){
		super(Order.class,true);
	}
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Order aOrder=(Order) a;
		Order bOrder=(Order) b;
		return aOrder.getId().compareTo(bOrder.getId());
	}
}

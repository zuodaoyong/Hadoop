package com.mr.reducejoin;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OrderMapper extends Mapper<LongWritable,Text,Text,OrderWrapper>{

	private OrderWrapper order=new OrderWrapper();
	private Text k=new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//区分两张表
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		//获取一行
		String line = value.toString();
		if(StringUtils.isNotEmpty(line)){
			
			if("order.txt".equals(fileName)){//订单表  1001 01 1
				String[] splits = line.split(" ");
				order.setId(splits[0]);
				order.setPid(splits[1]);
				order.setAmount(Integer.valueOf(splits[2]));
				order.setFlag("order");
				order.setPname("");
				k.set(splits[1]);
			}else{//产品表 01 小米
				String[] splits = line.split(" ");
				order.setPid(splits[0]);
				order.setPname(splits[1]);
				order.setId("");
				order.setAmount(0);
				order.setFlag("");
				k.set(splits[0]);
			}
			context.write(k,order);
		}
		
	}
}

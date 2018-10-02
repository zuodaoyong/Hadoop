package com.mr.reducejoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReduce extends Reducer<Text,OrderWrapper, OrderWrapper, NullWritable>{

	
	@Override
	protected void reduce(Text text, Iterable<OrderWrapper> iterable,
			Context context)
			throws IOException, InterruptedException {
		List<OrderWrapper> list=new ArrayList<>();
		OrderWrapper pdDest=new OrderWrapper();
		for(OrderWrapper wrapper:iterable){
			OrderWrapper dest=new OrderWrapper();
			if("order".equals(wrapper.getFlag())){
				try {
					BeanUtils.copyProperties(dest,wrapper);
					list.add(dest);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}else{
				try {
					BeanUtils.copyProperties(pdDest,wrapper);
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
		}
		
		//遍历list
		for(OrderWrapper orderWrapper:list){
			orderWrapper.setPname(pdDest.getPname());
			context.write(orderWrapper, NullWritable.get());
		}
	}
	
}

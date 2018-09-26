package com.mr.phoneflow;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PhoneFlowReduce extends Reducer<Text, PhoneFlow, Text, Text>{

	private PhoneFlow result;
	@Override
	protected void reduce(Text text, Iterable<PhoneFlow> list, Reducer<Text, PhoneFlow, Text, Text>.Context context)
			throws IOException, InterruptedException {
		result=new PhoneFlow();
		init(result);
		Iterator<PhoneFlow> iterator = list.iterator();
		result.setPhone(text.toString());
		while(iterator.hasNext()){
			PhoneFlow next = iterator.next();
			result.setLowFlow(result.getLowFlow()+next.getLowFlow());
			result.setUpFlow(result.getUpFlow()+next.getUpFlow());
		}
		result.setTotalFlow(result.getLowFlow()+result.getUpFlow());
		context.write(text, new Text(result.getLowFlow()+","+result.getUpFlow()+","+result.getTotalFlow()));
	}
	
	private void init(PhoneFlow result){
		result.setLowFlow(0L);
		result.setTotalFlow(0L);
		result.setUpFlow(0L);
	}
}

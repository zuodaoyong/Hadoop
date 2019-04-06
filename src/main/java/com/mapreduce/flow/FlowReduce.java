package com.mapreduce.flow;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowReduce extends Reducer<Text,PhoneFlow,PhoneFlow, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<PhoneFlow> values, Context context) throws IOException, InterruptedException {
        //1、获取每个手机号对应的值
        Iterator<PhoneFlow> iterator = values.iterator();
        PhoneFlow pf=new PhoneFlow();
        long lowFlow=0L;
        long upFlow=0L;
        while(iterator.hasNext()){
            PhoneFlow next = iterator.next();
            lowFlow+=next.getLowFlow();
            upFlow+=next.getUpFlow();
        }
        pf.setPhone(key.toString());
        pf.setLowFlow(lowFlow);
        pf.setUpFlow(upFlow);
        pf.setTotalFlow(lowFlow+upFlow);
        context.write(pf,NullWritable.get());
    }
}

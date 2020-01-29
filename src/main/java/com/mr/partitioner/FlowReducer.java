package com.mr.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowReducer extends Reducer<Text,Flow,Text,Flow> {

    @Override
    protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
        Long upFlow=0L;
        Long downFlow=0L;
        Long sumFlow=0L;
        Iterator<Flow> iterator = values.iterator();
        while (iterator.hasNext()){
            Flow next = iterator.next();
            upFlow+=next.getUpFlow();
            downFlow+=next.getDownFlow();
            sumFlow+=next.getSumFlow();
        }
        Flow flow=new Flow();
        flow.setSumFlow(sumFlow);
        flow.setDownFlow(downFlow);
        flow.setUpFlow(upFlow);
        context.write(key,flow);
    }
}

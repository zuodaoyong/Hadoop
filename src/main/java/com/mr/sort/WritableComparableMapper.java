package com.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WritableComparableMapper extends Mapper<LongWritable, Text, Flow,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Flow flow=new Flow();
        String[] split = value.toString().split("\t");
        String phoneNum = split[1];
        flow.setUpFlow(Long.valueOf(split[split.length-3]));
        flow.setDownFlow(Long.valueOf(split[split.length-2]));
        flow.setSumFlow(flow.getUpFlow()+flow.getDownFlow());
        context.write(flow,new Text(phoneNum));
    }
}

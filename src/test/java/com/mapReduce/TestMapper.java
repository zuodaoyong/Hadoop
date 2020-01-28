package com.mapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper<LongWritable, Text,Text,Flow> {

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        Flow flow=new Flow();
        String[] split = line.toString().split("\t");
        String phoneNum = split[1];
        flow.setUpFlow(Long.valueOf(split[split.length-3]));
        flow.setDownFlow(Long.valueOf(split[split.length-2]));
        flow.setSumFlow(flow.getUpFlow()+flow.getDownFlow());
        context.write(new Text(phoneNum),flow);
    }
}

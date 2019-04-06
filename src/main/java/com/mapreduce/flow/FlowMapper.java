package com.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,PhoneFlow> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、获取每行
        String line=value.toString();
        //2、切割
        String[] splits = line.split("\\s");
        PhoneFlow phoneFlow=new PhoneFlow();
        phoneFlow.setPhone(splits[0]);
        phoneFlow.setUpFlow(Long.valueOf(splits[1]));
        phoneFlow.setLowFlow(Long.valueOf(splits[2]));
        phoneFlow.setTotalFlow(Long.valueOf(splits[1])+Long.valueOf(splits[2]));
        //3、发射
        context.write(new Text(splits[0]),phoneFlow);
    }
}

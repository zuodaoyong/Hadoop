package com.mr.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineInputFormatMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for(int i=0;i<split.length;i++){
            context.write(new Text(split[i]),new LongWritable(1));
        }
    }
}

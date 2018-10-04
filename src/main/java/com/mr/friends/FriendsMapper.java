package com.mr.friends;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendsMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context)
    		throws IOException, InterruptedException {
    	String line = value.toString();
    	String[] splits = line.split(":");
    	String[] arr = splits[1].split(",");
    	for(String a:arr){
    		context.write(new Text(a),new Text(splits[0]+a));
    	}
    }
}

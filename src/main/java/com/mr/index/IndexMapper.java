package com.mr.index;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class IndexMapper extends Mapper<LongWritable,Text,Text, Index>{

	Text k=new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] splits = line.split("\\s");
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		Arrays.asList(splits).stream()
		      .forEach(e->{
		    	  k.set(e);
		    	  Index index=new Index();
		    	  try {
		    		  index.setName(e);
		    		  index.setPath(fileName);
		    		  index.setCount(1);
					  context.write(k,index);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
		      });
	}
}

package com.mr.index2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoIndexReduce extends Reducer<Text,Text,Text,Text>{

	@Override
	protected void reduce(Text text, Iterable<Text> iterable,Context context)
			throws IOException, InterruptedException {
		StringBuilder sb=new StringBuilder();
		for(Text textStr:iterable){
			sb.append(textStr.toString().replace("\t","->")).append(" ");
		}
		context.write(text, new Text(sb.toString()));
	}
}

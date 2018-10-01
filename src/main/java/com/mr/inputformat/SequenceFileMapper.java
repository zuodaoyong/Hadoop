package com.mr.inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{

	private Text key=new Text();
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//获取文件的路径和名称
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		Path path = fileSplit.getPath();
		key.set(path.toString());
	}
	
	@Override
	protected void map(NullWritable key, BytesWritable value,
			Context context)
			throws IOException, InterruptedException {
		context.write(this.key,value);
	}
}

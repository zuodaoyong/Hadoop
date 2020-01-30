package com.mr.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FilterRecordWriter extends RecordWriter<Text,NullWritable>{

	private FSDataOutputStream hadoopOutputStream=null;
	private FSDataOutputStream otherOutputStream=null;
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		IOUtils.closeStream(hadoopOutputStream);
		IOUtils.closeStream(otherOutputStream);
	}

	@Override
	public void write(Text text, NullWritable writable) throws IOException, InterruptedException {
		if(text.toString().contains("www.123.com")){
			hadoopOutputStream.write(text.toString().getBytes());
		}else{
			otherOutputStream.write(text.toString().getBytes());
		}
	}

	public FilterRecordWriter(TaskAttemptContext context) {
		FileSystem fileSystem=null;
		try {
			//获取文件系统
			fileSystem = FileSystem.get(context.getConfiguration());
			//创建输出文件路径
			Path hadoopPath = new Path("/mapreduce/outputFormat/output/123.log");
			Path otherPath = new Path("/mapreduce/outputFormat/output/other.log");
			hadoopOutputStream=fileSystem.create(hadoopPath);
			otherOutputStream=fileSystem.create(otherPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

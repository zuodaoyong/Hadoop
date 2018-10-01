package com.mr.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FilterRecordWriter extends RecordWriter<Text,NullWritable>{

	private FSDataOutputStream hadoopOutputStream=null;
	private FSDataOutputStream otherOutputStream=null;
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		if(hadoopOutputStream!=null){
			hadoopOutputStream.close();
		}
		if(otherOutputStream!=null){
			otherOutputStream.close();
		}
	}

	@Override
	public void write(Text text, NullWritable writable) throws IOException, InterruptedException {
		if(text.toString().contains("hadoop")){
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
			Path hadoopPath = new Path("D:\\software\\temp\\hadoop\\outputFormat\\hadoop.txt");
			Path otherPath = new Path("D:\\software\\temp\\hadoop\\outputFormat\\other.txt");
			hadoopOutputStream=fileSystem.create(hadoopPath);
			otherOutputStream=fileSystem.create(otherPath);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fileSystem!=null){
				try {
					fileSystem.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}

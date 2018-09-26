package com.mr.phoneflow;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PhoneFlowDriver {

	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(PhoneFlowDriver.class);
		job.setMapperClass(PhoneFlowMaper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PhoneFlow.class);
		job.setReducerClass(PhoneFlowReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//设置分区
		job.setPartitionerClass(ProvincePartition.class);
		job.setNumReduceTasks(3);
		FileInputFormat.setInputPaths(job, new Path("E:\\学习文档\\hadoop\\data\\input\\phoneflow\\phoneflow.txt"));
	    FileOutputFormat.setOutputPath(job, new Path("E:\\学习文档\\hadoop\\data\\phoneflow"));
	    boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

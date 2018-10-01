package com.mr.groupingcomparator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-2.7.2");
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setMapperClass(OrderMapper.class);
		job.setMapOutputKeyClass(Order.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(OrderReduce.class);
		job.setOutputKeyClass(Order.class);
		job.setOutputValueClass(NullWritable.class);
		//设置分区
		job.setPartitionerClass(OrderPartition.class);
		job.setNumReduceTasks(3);
		//添加grouping
		job.setGroupingComparatorClass(OrderGroupingCompartor.class);
		FileInputFormat.setInputPaths(job, new Path("D:\\software\\temp\\hadoop\\groupingComparator"));
	    FileOutputFormat.setOutputPath(job, new Path("D:\\software\\temp\\hadoop\\groupingComparator\\output"));
	    boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

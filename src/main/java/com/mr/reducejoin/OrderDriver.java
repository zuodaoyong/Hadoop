package com.mr.reducejoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setMapperClass(OrderMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderWrapper.class);
		job.setReducerClass(OrderReduce.class);
		job.setOutputKeyClass(OrderWrapper.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/mapreduce/join"));
		FileOutputFormat.setOutputPath(job, new Path("/mapreduce/join/output"));
		boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

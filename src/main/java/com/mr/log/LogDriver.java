package com.mr.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LogDriver {
    public static void main(String[] args) throws Exception{
		System.setProperty("HADOOP_USER_NAME", "root");
    	Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setMapperClass(LogMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path("/mapreduce/log/web"));
		FileOutputFormat.setOutputPath(job, new Path("/mapreduce/log/output"));
		boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}	
}

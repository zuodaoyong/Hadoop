package com.mr.index2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OneIndexDriver {

	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setMapperClass(OneIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(OneIndexReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("D:\\software\\temp\\hadoop\\index"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\software\\temp\\hadoop\\index\\output"));
		boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

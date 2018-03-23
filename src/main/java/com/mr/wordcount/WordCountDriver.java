package com.mr.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("E:\\学习文档\\hadoop\\data\\input\\wordcount\\wordcount.txt"));
	    FileOutputFormat.setOutputPath(job, new Path("E:\\学习文档\\hadoop\\data\\output\\wordcount\\wordcountOutput.txt"));
	    boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	    
	}
}

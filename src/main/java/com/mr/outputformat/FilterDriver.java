package com.mr.outputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterDriver {

	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setOutputFormatClass(FilterOutputFormat.class);
		job.setMapperClass(FilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(FilterReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("D:\\software\\temp\\hadoop\\outputFormat"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\software\\temp\\hadoop\\outputFormat\\output"));
		boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

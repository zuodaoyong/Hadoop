package com.mr.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SequenceFileDriver {

	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setInputFormatClass(WholeInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(SequenceFileReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		FileInputFormat.setInputPaths(job, new Path("D:\\software\\temp\\hadoop\\sequenceFile"));
	    FileOutputFormat.setOutputPath(job, new Path("D:\\software\\temp\\hadoop\\sequenceFile\\output"));
	    boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

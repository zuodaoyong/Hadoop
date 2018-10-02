package com.mr.mapjoin;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mr.reducejoin.OrderWrapper;

public class DistributedCacheDriver {

	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setMapperClass(DistributedCacheMapper.class);
		job.setMapOutputKeyClass(OrderWrapper.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//添加缓存数据
		job.addCacheFile(new URI("file:///D:/software/temp/hadoop/join/pd.txt"));
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path("D:\\software\\temp\\hadoop\\join\\order.txt"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\software\\temp\\hadoop\\join\\output"));
		boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

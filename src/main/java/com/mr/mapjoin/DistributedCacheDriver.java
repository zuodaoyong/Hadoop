package com.mr.mapjoin;

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
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setMapperClass(DistributedCacheMapper.class);
		job.setMapOutputKeyClass(OrderWrapper.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//添加缓存数据
		job.addCacheFile(new URI("/mapreduce/join/pd"));
		//Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path("/mapreduce/join/order"));
		FileOutputFormat.setOutputPath(job, new Path("/mapreduce/join/output"));
		boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

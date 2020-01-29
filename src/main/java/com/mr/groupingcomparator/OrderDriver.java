package com.mr.groupingcomparator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 求出每一个订单中最贵的商品
 * 输入数据(groupingCompartor.txt):
 * 0000001	Pdt_01	222.8
 * 0000002	Pdt_05	722.4
 * 0000001	Pdt_02	33.8
 * 0000003	Pdt_06	232.8
 * 0000003	Pdt_02	33.8
 * 0000002	Pdt_03	522.8
 * 0000002	Pdt_04	122.4
 * 期望输出数据
 * 1	222.8
 * 2	722.4
 * 3	232.8
 * 需求分析：
 * （1）利用“订单id和成交金额”作为key，
 * 可以将Map阶段读取到的所有订单数据按照id升序排序，如果id相同再按照金额降序排序，发送到Reduce。
 * （2）在Reduce端利用groupingComparator将订单id相同的kv聚合成组，
 * 然后取第一个即是该订单中最贵商品
 */
public class OrderDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setMapperClass(OrderMapper.class);
		job.setMapOutputKeyClass(Order.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(OrderReduce.class);
		job.setOutputKeyClass(Order.class);
		job.setOutputValueClass(NullWritable.class);
		//设置分区
		//job.setPartitionerClass(OrderPartition.class);
		//job.setNumReduceTasks(3);
		//添加grouping
		job.setGroupingComparatorClass(OrderGroupingCompartor.class);
		FileInputFormat.setInputPaths(job, new Path("/mapreduce/sort/groupingCompartor"));
	    FileOutputFormat.setOutputPath(job, new Path("/mapreduce/sort/output"));
	    boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

package com.mr.inputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * 将多个小文件合并成一个SequenceFile文件（SequenceFile文件是Hadoop用来存储二进制形式的key-value对的文件格式），
 * SequenceFile里面存储着多个文件，存储的形式为文件路径+名称为key，文件内容为value。
 * 1.txt文件内容如下
 * yongpeng weidong weinan
 * sanfeng luozong xiaoming
 * 2.txt文件内容如下
 * longlong fanfan
 * mazong kailun yuhang yixin
 * longlong fanfan
 * mazong kailun yuhang yixin
 * 3.txt文件内容如下
 * shuaige changmo zhenqiang
 *  * dongli lingu xuanxuan
 */
public class SequenceFileDriver {

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		//设置输入的inputFormat
		job.setInputFormatClass(WholeInputFormat.class);
		//设置输出的outputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(SequenceFileReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/mapreduce/inputformat/sequencefiles"));
	    FileOutputFormat.setOutputPath(job, new Path("/mapreduce/inputformat/output"));
	    boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

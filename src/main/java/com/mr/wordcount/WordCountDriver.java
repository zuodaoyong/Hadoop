package com.mr.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-2.7.2");
		Configuration configuration=new Configuration();
		//开启map端压缩
		configuration.setBoolean("mapreduce.map.output.compress",true);
		configuration.setClass("mapreduce.map.output.compress.codec",BZip2Codec.class, CompressionCodec.class);
		Job job = Job.getInstance(configuration);
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//将小文件合并成大文件
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 128*1024*102);
		CombineTextInputFormat.setMinInputSplitSize(job, 128*1024*102);
		//设置combiner
		//job.setCombinerClass(WordCountCombiner.class);//WordCountCombiner和WordCountReduce的业务一样，所以不用特地去写combiner类
		job.setCombinerClass(WordCountReduce.class);
		//设置reduce端压缩开启
		FileOutputFormat.setCompressOutput(job, true);
		//设置压缩方式
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:\\software\\temp\\hadoop\\wordcount"));
	    FileOutputFormat.setOutputPath(job, new Path("D:\\software\\temp\\hadoop\\wordcount\\output"));
	    boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	    
	}
}

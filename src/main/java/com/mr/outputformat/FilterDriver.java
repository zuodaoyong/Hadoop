package com.mr.outputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 过滤输入的log日志，包含www.123.com的网站输出到123.log，不包含www.123.com的网站输出到other.log。
 * http://www.baidu.com
 * http://www.google.com
 * http://cn.bing.com
 * http://www.123.com
 * http://www.sohu.com
 * http://www.sina.com
 * http://www.sin2a.com
 * http://www.sin2desa.com
 * http://www.sindsafa.com
 *
 */
public class FilterDriver {

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration configuration=new Configuration();
		Job job = Job.getInstance(configuration);
		job.setOutputFormatClass(FilterOutputFormat.class);
		job.setMapperClass(FilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(FilterReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/mapreduce/outputFormat/log"));
		FileOutputFormat.setOutputPath(job, new Path("/mapreduce/outputFormat/output"));
		boolean waitForCompletion = job.waitForCompletion(true);
	    System.exit(waitForCompletion==true?0:1);
	}
}

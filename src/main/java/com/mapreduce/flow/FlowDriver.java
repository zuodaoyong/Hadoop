package com.mapreduce.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * 手机流量
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);
        job.setJarByClass(FlowDriver.class);
        //设置Mapper,reduce
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);
        //设置输入输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneFlow.class);
        job.setOutputKeyClass(PhoneFlow.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入和输出文件的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean bool = job.waitForCompletion(true);
        System.exit(bool?0:1);
    }
}

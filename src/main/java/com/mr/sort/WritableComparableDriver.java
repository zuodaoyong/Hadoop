package com.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WritableComparableDriver {

    public static void main(String[] args) throws Exception{

        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WritableComparableDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(WritableComparableMapper.class);
        job.setReducerClass(WritableComparableReducer.class);
        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Flow.class);
        job.setMapOutputValueClass(Text.class);
        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);
        //当不设置分区器，则是全排序，设置分区器是区内排序
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);
        //指定job的输入原始文件所在目录和输出文件目录
        FileInputFormat.setInputPaths(job,new Path("\\mapreduce\\flow\\phoneflow"));
        FileOutputFormat.setOutputPath(job,new Path("\\mapreduce\\flow\\output"));
        boolean completion = job.waitForCompletion(true);
        System.exit(completion?0:1);

    }
}

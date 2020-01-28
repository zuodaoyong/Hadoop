package com.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class TestDriver {
    public static void main(String[] args) throws Exception{

        //获取配置信息，job对象实例
        Configuration configuration=new Configuration();
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,"\t");
        System.setProperty("HADOOP_USER_NAME", "root");
        Job job=Job.getInstance(configuration);
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(TestDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);
        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow.class);
        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);
        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("\\mapreduce\\flow\\phoneflow"));
        FileOutputFormat.setOutputPath(job,new Path("\\mapreduce\\flow\\output"));
        //将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}

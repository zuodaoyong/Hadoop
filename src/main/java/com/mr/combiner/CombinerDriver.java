package com.mr.combiner;

import com.mr.wordcount.WordCountMapper;
import com.mr.wordcount.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombinerDriver {

    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(CombinerDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置combiner
        job.setCombinerClass(WordcountCombiner.class);
        FileInputFormat.setInputPaths(job,new Path("/mapreduce/wordcount/wordcount"));
        FileOutputFormat.setOutputPath(job,new Path("/mapreduce/wordcount/output"));
        boolean completion = job.waitForCompletion(true);
        System.exit(completion?0:1);
    }
}

package com.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计输入文件中每一行的第一个单词相同的行数。
 * （1）输入数据
 * banzhang ni hao
 * xihuan hadoop banzhang
 * banzhang ni hao
 * xihuan hadoop banzhang
 * （2）期望结果数据
 * banzhang	2
 * xihuan	2
 */
public class KeyValueTextInputFormatDriver {
    public static void main(String[] args) throws Exception{

        Configuration configuration=new Configuration();
        System.setProperty("HADOOP_USER_NAME", "root");
        // 设置切割符
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(configuration);

        //设置jar包位置，关联mapper和reducer
        job.setJarByClass(KeyValueTextInputFormatDriver.class);
        job.setMapperClass(KeyValueTextInputFormatMapper.class);
        job.setReducerClass(KeyValueTextInputFormatReducer.class);
        //设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置输入输出数据路径
        FileInputFormat.setInputPaths(job,new Path("/mapreduce/inputformat/keyvaluetextinputformat"));
        //设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //设置输出数据路径
        FileOutputFormat.setOutputPath(job,new Path("/mapreduce/inputformat/output"));
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }
}

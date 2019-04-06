package com.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * wordcountmapper
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、获取每行
        String line=value.toString();
        //2、切割单词
        String[] splits = line.split("\\s");
        //3、发射到reduce
        Text word_key=new Text();
        IntWritable word_value=new IntWritable();
        for (String word:splits){
            word_key.set(word);
            word_value.set(1);
            context.write(word_key,word_value);
        }
    }
}

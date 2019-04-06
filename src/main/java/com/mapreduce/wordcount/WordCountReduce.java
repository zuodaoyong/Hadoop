package com.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * wordcountreduce
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        IntWritable word_value=new IntWritable();
        //1、获取每组值相加
        Iterator<IntWritable> iterator = values.iterator();
        while(iterator.hasNext()){
             sum+=iterator.next().get();
        }
        word_value.set(sum);
        context.write(key,word_value);
    }
}

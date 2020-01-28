package com.mr.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class NLineInputFormatReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long sum=0L;
        Iterator<LongWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            sum+=iterator.next().get();
        }
        context.write(key,new LongWritable(sum));
    }
}

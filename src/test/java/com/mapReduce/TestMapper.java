package com.mapReduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper<LongWritable, Text, NullWritable,Text> {

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String etlString = oriString2ETLString(line.toString());
        if(StringUtils.isBlank(etlString))
            return;
        context.write(NullWritable.get(),new Text(etlString));
    }


    public static String oriString2ETLString(String ori){
        StringBuilder etlString = new StringBuilder();
        String[] splits = ori.split("\t");
        if(splits.length < 9)
            return null;
        splits[3] = splits[3].replace(" ", "");
        for(int i = 0; i < splits.length; i++){
            if(i < 9){
                if(i == splits.length - 1){
                    etlString.append(splits[i]);
                }else{
                    etlString.append(splits[i] + "\t");
                }
            }else{
                if(i == splits.length - 1){
                    etlString.append(splits[i]);
                }else{
                    etlString.append(splits[i] + "&");
                }
            }
        }
        return etlString.toString();
    }
}

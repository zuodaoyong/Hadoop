package com.mr.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, Flow> {
    @Override
    public int getPartition(Text key, Flow flow, int i) {
        // 1 获取电话号码的前三位
        String preNum = key.toString().substring(0, 3);
        int partition = 5;
        // 2 判断是哪个省
        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }else{
            partition=4;
        }
        return partition;
    }
}

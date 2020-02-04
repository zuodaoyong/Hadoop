package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class LowerUDF extends UDF{

    /**
     * 将输入值变成小写格式
     * @param input
     * @return
     */
    public String evaluate(String input){
        return input.toLowerCase();
    }

    /**
     * input变成小写后拼接上defaultV
     * @param input
     * @param defaultV
     * @return
     */
    public String evaluate(String input,String defaultV){
        return input.toLowerCase()+defaultV;
    }
}

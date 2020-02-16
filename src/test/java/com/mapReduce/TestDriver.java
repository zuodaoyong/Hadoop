package com.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestDriver implements Tool {
    Configuration configuration=null;
    public static void main(String[] args) throws Exception{
        int run = ToolRunner.run(new TestDriver(), args);
        System.exit(run);
    }


    @Override
    public int run(String[] strings) throws Exception {
        //获取配置信息，job对象实例
        Configuration configuration=new Configuration();
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,"\t");
        System.setProperty("HADOOP_USER_NAME", "root");
        Job job=Job.getInstance(configuration);
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(TestDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(TestMapper.class);
        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("/test/video"));
        FileOutputFormat.setOutputPath(job,new Path("/test/output"));
        //将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        //System.exit(result ? 0 : 1);
        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration=configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}

package com.mr.mapjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.mr.reducejoin.OrderWrapper;

public class DistributedCacheMapper extends Mapper<LongWritable, Text,OrderWrapper, NullWritable>{

	Map<String,String> map=new HashedMap<>();
	private BufferedReader bufferedReader;
	private String[] splits;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//获取缓存的文件
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		FSDataInputStream hdfsInStream = fileSystem.open(new Path(path));
		bufferedReader = new BufferedReader(new InputStreamReader(hdfsInStream, "UTF-8"));
		String line;
		while(StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
			// 2 切割
			String[] fields = line.split("\t");
			// 3 缓存数据到集合
			map.put(fields[0], fields[1]);
		}
		//关闭流
		bufferedReader.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		if(!"pd".equals(fileName)){
			String line = value.toString();
			if(StringUtils.isNotEmpty(line)){
				String[] splits = line.split("\t");
				OrderWrapper wrapper=new OrderWrapper();
				wrapper.setId(splits[0]);
				wrapper.setPid(splits[1]);
				wrapper.setAmount(Integer.valueOf(splits[2]));
				wrapper.setPname(map.get(splits[1]));
				wrapper.setFlag("");
				context.write(wrapper, NullWritable.get());
			}
		}
	}
}

package com.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSTest {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.15.129:8020");
		FileSystem fs=FileSystem.get(conf);
		boolean bool = fs.createNewFile(new Path("/test/test1.txt"));
	    System.out.println(bool);
	    fs.close();
	}
}

package com.test;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSTest {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.237.129:8020");
		FileSystem fs=FileSystem.get(conf);
		//fs.mkdirs(new Path("/test"));
		//boolean bool = fs.createNewFile(new Path("/test/test1.txt"));
	    //System.out.println(bool);
		uploadFile(fs);
	    fs.close();
	}
	
	@SuppressWarnings("static-access")
	public static void uploadFile(FileSystem fs) throws Exception{
		URL url = HDFSTest.class.getClassLoader().getSystemResource("excels/11.xlsx");
		Path src=new Path(url.toURI());
		fs.copyFromLocalFile(src, new Path("/test/11.xlsx"));
	}
}

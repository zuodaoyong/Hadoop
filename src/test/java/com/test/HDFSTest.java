package com.test;

import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

public class HDFSTest {

	private static FileSystem init1() throws Exception{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.15.129:8020");
		FileSystem fs=FileSystem.get(conf);
		return fs;
	}
	private static FileSystem init2() throws Exception{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://192.168.15.129:8020"), conf, "root");
		return fs;
	}
	public static void main(String[] args) throws Exception {
		
		FileSystem fs = init2();
		//fs.mkdirs(new Path("/test"));
		//boolean bool = fs.createNewFile(new Path("/test/test1.txt"));
	    //System.out.println(bool);
		//uploadFile(fs);
		//deleteFile(fs);
		createFile(fs);
		
		//fs.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, progress)
	    fs.close();
	}
	
	public static void createFile(FileSystem fs) throws Exception{
		Path path = new Path("/test1/test.java");
		FSDataOutputStream create = fs.create(path, true);
		//fs.
		create.writeUTF("test123");
		create.flush();
		//create.close();
		System.out.println("end...");
	}
	
	@SuppressWarnings("static-access")
	public static void uploadFile(FileSystem fs) throws Exception{
		URL url = HDFSTest.class.getClassLoader().getSystemResource("excels/11.xlsx");
		Path src=new Path(url.toURI());
		fs.copyFromLocalFile(src, new Path("/test/11.xlsx"));
	}
	
	public static void deleteFile(FileSystem fs) throws Exception{
		Path path=new Path("/test/test1.txt");
		boolean deleteOnExit = fs.deleteOnExit(path);
		System.out.println(deleteOnExit);
	}
}

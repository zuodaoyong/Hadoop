package com.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoRequestProto;
import org.apache.hadoop.io.IOUtils;

public class HDFSTest {

	private static FileSystem init() throws Exception{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://spark1:8020"), conf, "root");
		return fs;
	}
	public static void main(String[] args) throws Exception {
		FileSystem fs = init();
		//uploadFile(fs);
		downFile(fs);
		fs.close();
	}
	
	public static void uploadFile(FileSystem fs) throws IllegalArgumentException, IOException{
		Configuration conf=new Configuration();
		InputStream in = new FileInputStream(new File("D://12.txt"));
	    FSDataOutputStream dataOutputStream = fs.create(new Path("/test/12.txt"));
	    IOUtils.copyBytes(in, dataOutputStream, conf);
	    in.close();
	    dataOutputStream.close();
	}
	
	public static void downFile(FileSystem fs) throws IllegalArgumentException, IOException{
		Configuration conf=new Configuration();
		FSDataInputStream dataInputStream = fs.open(new Path("/test/12.txt"));
		OutputStream out = new FileOutputStream(new File("D://1212.txt"));
	    IOUtils.copyBytes(dataInputStream, out, conf);
	    out.close();
	    dataInputStream.close();
	}
}

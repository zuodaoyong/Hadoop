package com.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsInit {

	public static FileSystem getFileSystem(Configuration conf) throws IOException, InterruptedException, URISyntaxException{
		
		return FileSystem.get(new URI("hdfs://spark1:8020"), conf, "root");
	}
}

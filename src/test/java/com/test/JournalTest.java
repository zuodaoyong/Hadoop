package com.test;

import java.io.IOException;


import com.common.HDFSUtils;

public class JournalTest {

	public static void main(String[] args) throws Exception {
		
		//HDFSUtils.mkdir("/zdy");
		//HDFSUtils.createFile("/zdy","hello","hello");
		//HDFSUtils.uploadFile("E:\\tmp\\hello1.txt", "/zdy/hello1.txt");
		//HDFSUtils.downFile("/zdy/hello1.txt","D://");
		//HDFSUtils.listFileStatus("/zdy/hello1.txt");
		//HDFSUtils.mergeSmallFile("E:\\tmp\\test", "/zdy/merge");
		HDFSUtils.readMergeSmallFile("/zdy/merge");
	}
}

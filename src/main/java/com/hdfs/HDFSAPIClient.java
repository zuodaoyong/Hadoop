package com.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 通过API的方式操作hdfs
 */
public class HDFSAPIClient {

	FileSystem fileSystem;
	
	@Before
	public void testBefore() throws IOException, InterruptedException, URISyntaxException{
		Configuration conf=new Configuration();
		fileSystem = HdfsInit.getFileSystem(conf);
	}
	
	/**
	 * 上传文件
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException{
		Configuration conf=new Configuration();
		conf.set("dfs.replication", "2");//配置两个副本
		fileSystem = HdfsInit.getFileSystem(conf);
		fileSystem.copyFromLocalFile(new Path("E:\\flow.txt"),new Path("/test/flow.txt"));
		System.out.println("上传成功");
	}
	
	/**
	 * 文件下载
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{
		fileSystem.copyToLocalFile(new Path("/test/flow.txt"), new Path("E:\\flow2.txt"));
		System.out.println("下载成功");
		fileSystem.close();
	}
	
	/**
	 * 创建目录
	 * @throws IllegalArgumentException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void testMkdirDirectory() throws IllegalArgumentException, IOException, InterruptedException, URISyntaxException{
		fileSystem.mkdirs(new Path("/test/test2"));
		System.out.println("创建目录成功");
	}
	
	/**
	 * 删除目录  当设置false时，如果删除的目录里有下级目录或文件则抛出异常
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void testDeleteDirectory() throws IOException, InterruptedException, URISyntaxException{
		fileSystem.delete(new Path("/test/test3"),false);
	}
	
	/**
	 * 修改文件名称
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRename() throws IllegalArgumentException, IOException{
		fileSystem.rename(new Path("/test/flow.txt"),new Path("/test/flow2.txt"));
		System.out.println("修改成功");
	}
	
	@Test
	public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException{
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/test/flow2.txt"), true);
		while(listFiles.hasNext()){
			LocatedFileStatus next = listFiles.next();
			System.out.println(next.getGroup()+":"+next.getOwner()+":"+next.getReplication()+":"+next.getPermission());
		}
		
	}
	
	@After
	public void testAfter() throws IOException{
		System.out.println("关闭资源");
		fileSystem.close();
	}
}

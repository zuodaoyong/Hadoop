package com.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSUtils {

	private static Logger logger=LoggerFactory.getLogger(HDFSUtils.class);
	private static FileSystem fileSystem;
	private static Configuration configuration;
	
	static{
		configuration=new Configuration();
		try {
			fileSystem=FileSystem.get(new URI(""), configuration,"root");
		} catch (Exception e) {
			logger.error("init fileSystem", e);
		}
	}
	
	/**
	 * 创建目录
	 * @throws IOException 
	 */
	public static void mkdir(String path) throws IOException{
		Path path2=new Path(path);
		FsPermission filePermission = new FsPermission(
                FsAction.ALL, //user action
                FsAction.ALL, //group action
                FsAction.READ);//other action
		fileSystem.mkdirs(path2, filePermission);
		logger.info("mkdir "+path+"success");
	}
	
	/**
	 * 创建文件
	 * @param path
	 * @param name
	 * @throws Exception
	 */
	public static void createFile(String path,String name,String content) throws Exception{
		Path path2=new Path(path+"/"+name);
		Progressable progressable=()->System.out.print("wait");
		FSDataOutputStream fsDataOutputStream = fileSystem.create(path2, progressable);
		fsDataOutputStream.writeBytes(content);
		fsDataOutputStream.hflush();
		fsDataOutputStream.close();
		logger.info(path+"/"+name+" create success");
	}
	
	/**
	 * 上传文件
	 * @param path
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static void uploadFile(String src,String dest) throws IllegalArgumentException, IOException{
		fileSystem.copyFromLocalFile(new Path(src),new Path(dest));
		logger.info(src+" uploadFile "+dest+" success");
	}
	
	/**
	 * 
	 * @param src
	 * @param dest
	 * @throws Exception 
	 */
	public static void downFile(String src,String dest) throws Exception{
	   fileSystem.copyToLocalFile(getPath(src),getPath(dest));
	   logger.info(src+"down "+dest+" success");
	}
	
	/**
	 * 查看元数据
	 * @param path
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void listFileStatus(String path) throws FileNotFoundException, IOException{
		RemoteIterator<LocatedFileStatus> locatedStatus = fileSystem.listLocatedStatus(getPath(path));
		while(locatedStatus.hasNext()){
			LocatedFileStatus fileStatus = locatedStatus.next();
			logger.info(JSON.toString(fileStatus));
		}
	}
	/**
	 * 合并小文件(旧版本写法)
	 * @param srcDirectory
	 * @throws IOException 
	 */
	public static void mergeSmallFile(String srcDirectory,String destPath) throws IOException{
		SequenceFile.Writer writer=SequenceFile.createWriter(fileSystem, configuration,getPath(destPath), Text.class, Text.class);
		File file = new File(srcDirectory);
		for(File f:file.listFiles()){
			writer.append(new Text(f.getName()),new Text(FileUtils.readFileToString(f)));
		}
		logger.info("mergeSmallFile success");
	}
	
	/**
	 * 合并小文件(新版本写法)
	 * @param srcDirectory
	 * @throws IOException 
	 */
	public static void mergeSmallFile_new(String srcDirectory,String destPath) throws IOException{
		SequenceFile.Writer.Option pathOption= Writer.file(getPath(destPath));
		SequenceFile.Writer.Option keyClassOption=Writer.keyClass(Text.class);
		SequenceFile.Writer.Option valueOption=Writer.valueClass(Text.class);
		SequenceFile.Writer writer=SequenceFile.createWriter(configuration, pathOption,keyClassOption,valueOption);
		File file = new File(srcDirectory);
		for(File f:file.listFiles()){
			writer.append(new Text(f.getName()),new Text(FileUtils.readFileToString(f)));
		}
		logger.info("mergeSmallFile success");
	}
	
	/**
	 * 读取合并的小文件（旧版本）
	 * @param srcPath
	 * @throws IOException
	 */
	public static void readMergeSmallFile(String srcPath) throws IOException{
		SequenceFile.Reader reader=new SequenceFile.Reader(fileSystem, getPath(srcPath), configuration);
	    Text key=new Text();
	    Text value=new Text();
	    while(reader.next(key,value)){
	    	System.out.println(key.toString());
	    	System.out.println(value.toString());
	    }
	    reader.close();
	}
	
	/**
	 * 读取合并的小文件（新版本）
	 * @param srcPath
	 * @throws IOException
	 */
	public static void readMergeSmallFile_new(String srcPath) throws IOException{
		SequenceFile.Reader.Option pathOption=SequenceFile.Reader.file(getPath(srcPath));
		SequenceFile.Reader reader=new SequenceFile.Reader(configuration, pathOption);
	    Text key=new Text();
	    Text value=new Text();
	    while(reader.next(key,value)){
	    	System.out.println(key.toString());
	    	System.out.println(value.toString());
	    }
	    reader.close();
	}
	private static Path getPath(String str){
		Path path=new Path(str);
		return path;
	}
}

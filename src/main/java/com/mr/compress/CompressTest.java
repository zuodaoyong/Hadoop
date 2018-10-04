package com.mr.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressTest {

	public static void main(String[] args) throws Exception {
		compress("D:\\software\\temp\\hadoop\\compress\\test.txt","org.apache.hadoop.io.compress.BZip2Codec");
	    decompress("D:\\software\\temp\\hadoop\\compress\\test.txt.bz2");
	}

	private static void decompress(String path) throws Exception{
		//校验
		CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
		CompressionCodec codec = codecFactory.getCodec(new Path(path));
		if(codec==null){
			System.out.println("不支持该解码器。。。");
			return;
		}
		//获取出入流
		CompressionInputStream inputStream = codec.createInputStream(new FileInputStream(new File(path)));
		//获取输出流
		FileOutputStream outputStream = new FileOutputStream(new File(path+".decode"));
		//拷贝流
		IOUtils.copyBytes(inputStream, outputStream,1024*1024*5,false);
		//关闭流
		inputStream.close();
		outputStream.close();
	}

	private static void compress(String path, String method) throws Exception {
		//获取输入流
		FileInputStream inputStream=new FileInputStream(new File(path));
		//获取输出流
		Class<?> clazz = Class.forName(method);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz,new Configuration());
		FileOutputStream outputStream = new FileOutputStream(new File(path+codec.getDefaultExtension()));
		CompressionOutputStream compressionOutputStream = codec.createOutputStream(outputStream);
		//流的拷贝
		IOUtils.copyBytes(inputStream, compressionOutputStream, 1024*1024*5,false);
		//关闭流
		inputStream.close();
		compressionOutputStream.close();
		outputStream.close();
	}
}

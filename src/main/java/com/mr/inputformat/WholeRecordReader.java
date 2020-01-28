package com.mr.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeRecordReader extends RecordReader<Text,BytesWritable>{

	private BytesWritable value=new BytesWritable();
	private Text key=new Text();
    private boolean isProcess=false;
    private FileSplit fileSplit;
    private Configuration configuration;
	@Override
	public void close() throws IOException {
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return isProcess?1:0;
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		fileSplit=(FileSplit) inputSplit;
		configuration = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if(!isProcess){
			FSDataInputStream inputStream=null;
			FileSystem fileSystem=null;
			try {
				byte[] bs=new byte[(int) fileSplit.getLength()];
				//获取文件系统
				Path path = fileSplit.getPath();
				fileSystem = path.getFileSystem(configuration);
				//打开文件流
				inputStream = fileSystem.open(path);
				IOUtils.readFully(inputStream, bs, 0,bs.length);
				value.set(bs, 0, bs.length);
                key.set(path.toString());
			}catch(Exception e){
				e.printStackTrace();
			}finally {
				if(inputStream!=null){
					inputStream.close();
				}
				if(fileSystem!=null){
					fileSystem.close();
				}
			}
			isProcess=true;
			return true;
		}
		
		return false;
	}

	
}

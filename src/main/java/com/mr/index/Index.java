package com.mr.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Index implements Writable{

	private String name;
	private String path;
	private int count;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	@Override
	public String toString() {
		return name + " " + path + " " + count;
	}
	@Override
	public void readFields(DataInput input) throws IOException {
		this.name=input.readUTF();
		this.path=input.readUTF();
		this.count=input.readInt();
	}
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(name);
		output.writeUTF(path);
		output.writeInt(count);
	}
	
}

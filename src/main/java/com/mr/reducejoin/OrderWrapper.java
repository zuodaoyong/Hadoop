package com.mr.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class OrderWrapper implements Writable{
	private String id;//order id
	private String pid;//pid
	private int amount;//order 数量
	private String pname;//pid对应的名称
	private String flag;//标记位
	@Override
	public void readFields(DataInput input) throws IOException {
		this.id = input.readUTF();
		this.pid=input.readUTF();
		this.amount=input.readInt();
		this.pname=input.readUTF();
		this.flag=input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(id);
		output.writeUTF(pid);
		output.writeInt(amount);
		output.writeUTF(pname);
		output.writeUTF(flag);
	}

	
	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	@Override
	public String toString() {
		return id + " " + pname + " "+amount;
	}

	
}

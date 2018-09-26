package com.mr.phoneflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PhoneFlow implements Writable,Comparable<PhoneFlow>{

	private String phone;//手机号
	private Long upFlow;//上行流量
	private Long lowFlow;//下行流量
	private Long totalFlow;//总流量
	
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public Long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(Long upFlow) {
		this.upFlow = upFlow;
	}

	public Long getLowFlow() {
		return lowFlow;
	}

	public void setLowFlow(Long lowFlow) {
		this.lowFlow = lowFlow;
	}

	public Long getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(Long totalFlow) {
		this.totalFlow = totalFlow;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeLong(upFlow);
		out.writeLong(lowFlow);
		out.writeLong(totalFlow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		phone=in.readUTF();
		upFlow=in.readLong();
		lowFlow=in.readLong();
		totalFlow=in.readLong();
	}

	@Override
	public int compareTo(PhoneFlow o) {
		return this.totalFlow>=o.totalFlow?-1:1;
	}
    

}

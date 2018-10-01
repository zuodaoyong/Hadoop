package com.mr.groupingcomparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Order implements WritableComparable<Order>{

	private String id;
	private float price;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public float getPrice() {
		return price;
	}
	public void setPrice(float price) {
		this.price = price;
	}
	@Override
	public void readFields(DataInput input) throws IOException {
		this.id = input.readUTF();
		this.price=input.readFloat();
	}
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(this.id);
		output.writeFloat(this.price);
	}
	@Override
	public int compareTo(Order o) {
		if(o.getId().equals(this.id)){
			return this.price>o.getPrice()?-1:1;
		}else{
			return this.id.compareTo(o.getId());
		}
		//return 0;
	}
	@Override
	public String toString() {
		return "Order [id=" + id + ", price=" + price + "]";
	}
	
	
}

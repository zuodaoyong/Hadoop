package com.mr.groupingcomparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Order implements WritableComparable<Order>{

	private String id;
	private Float price;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Float getPrice() {
		return price;
	}
	public void setPrice(Float price) {
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
			return o.getPrice().compareTo(this.getPrice());
		}else{
			return this.id.compareTo(o.getId());
		}
	}
	@Override
	public String toString() {
		return "Order [id=" + id + ", price=" + price + "]";
	}
	
	
}

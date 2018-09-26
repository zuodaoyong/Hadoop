package com.mr.phoneflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartition extends Partitioner<Text,PhoneFlow>{

	@Override
	public int getPartition(Text key, PhoneFlow value, int numPartitions) {
		String str = key.toString().substring(0,3);
		if("150".equals(str)){
			return 0;
		}else if("151".equals(str)){
			return 1;
		}
		return 2;
	}

}

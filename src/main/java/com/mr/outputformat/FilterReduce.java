package com.mr.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReduce extends Reducer<Text, NullWritable, Text, NullWritable>{

	@Override
	protected void reduce(Text text, Iterable<NullWritable> iterable,
			Context context) throws IOException, InterruptedException {
		String line = text.toString();
		line+="\r\n";
		context.write(new Text(line), NullWritable.get());
	}
}

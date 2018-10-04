package com.mr.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReduce extends Reducer<Text,Index,Index,NullWritable>{

	Map<String,Integer> map=new HashMap<>();
	@Override
	protected void reduce(Text text, Iterable<Index> iterable, Context context)
			throws IOException, InterruptedException {
		map.clear();
		Iterator<Index> iterator = iterable.iterator();
		while(iterator.hasNext()){
			Index next = iterator.next();
			String key = text.toString();
			String path = next.getPath();
			if(map.containsKey(key+":"+path)){
				Integer c = map.get(key+":"+path);
				map.put(key+":"+path, c+next.getCount());
			}else{
				map.put(key+":"+path,next.getCount());
			}
		}
		
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext()){
			String next = it.next();
			String[] splits = next.split(":");
			Index result=new Index();
			result.setName(splits[0]);
			result.setPath(splits[1]);
			result.setCount(map.get(next));
			context.write(result, NullWritable.get());
		}
	}
}

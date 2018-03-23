package com.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.functions.ReadFile;

public class CommonUtils {
	private static Logger logger=LoggerFactory.getLogger(CommonUtils.class);
	
	public static ReadFile<List<String[]>> readExcel=(inputStream)->{
		List<String[]> list=new ArrayList<>();
		BigExcelReader reader;
		try {
			reader = new BigExcelReader(inputStream) {
				@Override
				protected void outputRow(String[] datas, int[] rowTypes, int rowIndex) {
					if(rowIndex>0&&StringUtils.isNotEmpty(datas[0])){
						list.add(datas);
					}
				} 
			};
			// 执行解析  
	        reader.parse();  
		} catch (Exception e) {
			logger.error("CommonUtils->readExcel", e);
		}
		return list;
	};
	
	
	//读Excel
	public static Map<String,List<String[]>> readExcel(String dir,String suffix) throws Exception{
		return BaseReadFile.readFile(dir, suffix, readExcel);
	}
	
	public static void main(String[] args) throws Exception {
		Map<String, List<String[]>> map = readExcel("/excels",".xlsx");
		Iterator<Entry<String, List<String[]>>> iterator = map.entrySet().iterator();
		while(iterator.hasNext()){
			System.out.println(iterator.next().getValue());
		}
	}
	
}

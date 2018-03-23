package com.mr.callrecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcelParser {

	private static final Logger logger = LoggerFactory.getLogger(ExcelParser.class);

    /**
     * 解析is
     * 
     * @param is 数据源
     * @return String[]
     */
    public static String[] parseExcelData(InputStream is) {
        // 结果集
        List<String> resultList = new ArrayList<String>();
        
        try {
            // 获取Workbook
        	XSSFWorkbook workbook = new XSSFWorkbook(is);
            // 获取sheet
            XSSFSheet sheet = workbook.getSheetAt(0);
            
            Iterator<Row> rowIterator = sheet.iterator();
            
            while (rowIterator.hasNext()) {
                // 行
                Row row = rowIterator.next();
                // 字符串
                StringBuilder rowString = new StringBuilder();
                
                Iterator<Cell> colIterator = row.cellIterator();
                while (colIterator.hasNext()) {
                    Cell cell = colIterator.next();

                    switch (cell.getCellType()) {
                        case Cell.CELL_TYPE_BOOLEAN:
                            rowString.append(cell.getBooleanCellValue() + "\t");
                            break;
                        case Cell.CELL_TYPE_NUMERIC:
                            rowString.append(cell.getNumericCellValue() + "\t");
                            break;
                        case Cell.CELL_TYPE_STRING:
                            rowString.append(cell.getStringCellValue() + "\t");
                            break;
                    }
                }
                
                resultList.add(rowString.toString());
            }
        } catch (IOException e) {
            logger.error("IO Exception : File not found " + e);
        }
        
        return resultList.toArray(new String[0]);
    }
}

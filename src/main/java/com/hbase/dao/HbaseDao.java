package com.hbase.dao;

import com.hbase.constants.HbaseConstants;
import com.hbase.util.HbaseUtil;
import groovy.json.internal.Byt;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HbaseDao {


    public static void getList(String uid){
        System.out.println("=====================list begin===================================");
        Table table = HbaseUtil.getTable(HbaseConstants.content_table);
        Scan scan=new Scan();

        RowFilter rowFilter=new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(uid+"_"));
        scan.setFilter(rowFilter);
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell rawCell : result.rawCells()) {
                    System.out.println("rowkey is "+Bytes.toString(CellUtil.cloneRow(rawCell))+", family is"+Bytes.toString(CellUtil.cloneFamily(rawCell))+" , qualifier is "+Bytes.toString(CellUtil.cloneQualifier(rawCell))+" , value is "+Bytes.toString(CellUtil.cloneValue(rawCell)));
                }
            }
            System.out.println("=====================list end===================================");
        }catch (Exception e){
            log.error("getList is error", e);
        }


    }

    public static void getUidInfo(String uid) {
        System.out.println("=====================uidinfo begin===================================");
        Table inBoxTable = HbaseUtil.getTable(HbaseConstants.inbox_table);
        Table contentTable = HbaseUtil.getTable(HbaseConstants.content_table);
        Get get = new Get(Bytes.toBytes(uid));
        try {
            Result result = inBoxTable.get(get);
            for (Cell cell : result.rawCells()) {
                Get cellGet = new Get(CellUtil.cloneValue(cell));
                Result cellContent = contentTable.get(cellGet);
                for (Cell rawCell : cellContent.rawCells()) {
                    System.out.println("rowkey is "+Bytes.toString(CellUtil.cloneRow(rawCell))+", family is"+Bytes.toString(CellUtil.cloneFamily(rawCell))+" , qualifier is "+Bytes.toString(CellUtil.cloneQualifier(rawCell))+" , value is "+Bytes.toString(CellUtil.cloneValue(rawCell)));
                }
            }
            System.out.println("====================uidinfo end====================================");
        } catch (Exception e) {
            log.error("getUidInfo is error", e);
        }
    }

    public static void delAttends(String uid, String... attends) {
        Table relaTable = HbaseUtil.getTable(HbaseConstants.relation_table);
        List<Delete> deletes = new ArrayList<>();
        Delete delete = new Delete(Bytes.toBytes(uid));
        for (String attend : attends) {
            delete.addColumns(Bytes.toBytes(HbaseConstants.relation_table_cf1), Bytes.toBytes(attend));

            Delete attDel = new Delete(Bytes.toBytes(attend));
            attDel.addColumns(Bytes.toBytes(HbaseConstants.relation_table_cf2), Bytes.toBytes(uid));
            deletes.add(attDel);
        }
        deletes.add(delete);
        try {
            relaTable.delete(deletes);

            Table inBoxTable = HbaseUtil.getTable(HbaseConstants.inbox_table);

            Delete inBoxDel = new Delete(Bytes.toBytes(uid));

            for (String attend : attends) {
                inBoxDel.addColumns(Bytes.toBytes(HbaseConstants.inbox_table_cf), Bytes.toBytes(attend));
            }

            inBoxTable.delete(inBoxDel);
        } catch (Exception e) {
            log.error("delAttends is error", e);
        }

    }

    public static void publishWeiBo(String uid, String content) {
        String rowKey = addContent(uid, content);
        addInBox(uid, rowKey);
    }

    public static void addAttends(String uid, String... attends) {
        Table relaTable = HbaseUtil.getTable(HbaseConstants.relation_table);
        List<Put> relaPuts = new ArrayList<>();

        Put put = new Put(Bytes.toBytes(uid));

        for (String attend : attends) {
            put.addColumn(Bytes.toBytes(HbaseConstants.relation_table_cf1), Bytes.toBytes(attend), Bytes.toBytes(attend));
            Put attPut = new Put(Bytes.toBytes(attend));
            attPut.addColumn(Bytes.toBytes(HbaseConstants.relation_table_cf2), Bytes.toBytes(uid), Bytes.toBytes(uid));
            relaPuts.add(attPut);
        }
        relaPuts.add(put);
        try {
            relaTable.put(relaPuts);
            Table contentTable = HbaseUtil.getTable(HbaseConstants.content_table);
            Put inBoxPut = new Put(Bytes.toBytes(uid));
            for (String attend : attends) {
                Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
                ResultScanner tableScanner = contentTable.getScanner(scan);
                long ts = System.currentTimeMillis();
                for (Result result : tableScanner) {
                    inBoxPut.addColumn(Bytes.toBytes(HbaseConstants.inbox_table_cf), Bytes.toBytes(attend), ts++, result.getRow());
                }
            }
            if (!inBoxPut.isEmpty()) {
                Table inBoxTable = HbaseUtil.getTable(HbaseConstants.inbox_table);
                inBoxTable.put(inBoxPut);
            }

        } catch (Exception e) {
            log.error("addAttends is error", e);
        }
    }

    private static void addInBox(String uid, String rowKey) {
        try {
            Table table = HbaseUtil.getTable(HbaseConstants.relation_table);
            Get get = new Get(Bytes.toBytes(uid));
            get.addFamily(Bytes.toBytes(HbaseConstants.relation_table_cf2));
            Result result = table.get(get);

            List<Put> inBoxPuts = new ArrayList<>();
            for (Cell cell : result.rawCells()) {
                Put inBoxPut = new Put(CellUtil.cloneQualifier(cell));

                inBoxPut.addColumn(Bytes.toBytes(HbaseConstants.inbox_table_cf), Bytes.toBytes(uid), Bytes.toBytes(rowKey));

                inBoxPuts.add(inBoxPut);
            }

            if (inBoxPuts.size() > 0) {
                Table inBoxTable = HbaseUtil.getTable(HbaseConstants.inbox_table);
                inBoxTable.put(inBoxPuts);
            }
        } catch (Exception e) {
            log.error("addInBox is error", e);
        }


    }

    private static String addContent(String uid, String content) {
        Table contentTable = HbaseUtil.getTable(HbaseConstants.content_table);

        long currentTimeMillis = System.currentTimeMillis();

        String rowKey = uid + "_" + currentTimeMillis;

        Put contentPut = new Put(Bytes.toBytes(rowKey));

        contentPut.addColumn(Bytes.toBytes(HbaseConstants.content_table_cf), Bytes.toBytes("content"), Bytes.toBytes(content));

        try {
            contentTable.put(contentPut);
        } catch (Exception e) {
            log.error("publishWeiBo error e is {}", e.getLocalizedMessage());
        }
        return rowKey;
    }
}

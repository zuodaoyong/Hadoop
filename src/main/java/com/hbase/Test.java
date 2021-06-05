package com.hbase;

import com.hbase.constants.HbaseConstants;
import com.hbase.dao.HbaseDao;
import com.hbase.util.HbaseUtil;

public class Test {

    public static void main(String[] args) {

//        HbaseDao.publishWeiBo("1001","我是1001");
//        HbaseDao.getUidInfo("1001");
//
//        HbaseDao.addAttends("1001","1002","1003");
//
//        HbaseDao.publishWeiBo("1002","我是1002");
//        HbaseDao.publishWeiBo("1003","我是1003");
//        HbaseDao.getUidInfo("1001");
//
//        HbaseDao.delAttends("1001","1002");
        HbaseDao.getUidInfo("1001");
//
//        HbaseDao.addAttends("1001","1002");
        HbaseDao.getList("1001");
    }

    private static void init(){
        HbaseUtil.createTable(HbaseConstants.content_table,HbaseConstants.content_table_version,HbaseConstants.content_table_cf);
        HbaseUtil.createTable(HbaseConstants.relation_table,HbaseConstants.relation_table_version,HbaseConstants.relation_table_cf1,HbaseConstants.relation_table_cf2);
        HbaseUtil.createTable(HbaseConstants.inbox_table,HbaseConstants.inbox_table_version,HbaseConstants.inbox_table_cf);
    }


}

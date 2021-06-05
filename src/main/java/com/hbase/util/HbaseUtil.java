package com.hbase.util;

import javafx.scene.control.Tab;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

@Slf4j
public class HbaseUtil {

    private static Configuration configuration= HBaseConfiguration.create();

    public static Table getTable(String tablaName){
        Connection connection=null;
        Table table=null;
        try {
            connection = getConnection();
            table=connection.getTable(TableName.valueOf(tablaName));
        }catch (Exception e){
            close(connection,null);
        }
        return table;
    }

    public static void createNameSpace(String nameSpaces){
        Connection connection = getConnection();
        Admin admin=null;
        try {
            admin = connection.getAdmin();
            NamespaceDescriptor build = NamespaceDescriptor.create(nameSpaces).build();
            admin.createNamespace(build);
        }catch (Exception e){
            log.error("e is {}",e.getLocalizedMessage());
        }finally {
            close(connection,admin);
        }
    }

    public static boolean isExistTable(String tableName){
        Connection connection = getConnection();
        Admin admin=null;
        boolean bool=false;
        try {
           bool = admin.tableExists(TableName.valueOf(tableName));
        }catch (Exception e){
            close(connection,admin);
        }
        return bool;
    }

    public static void createTable(String tableName,int versions,String... cfs){
        if(cfs.length<=0){
            log.info("列族不能为空");
            return;
        }
        if(isExistTable(tableName)){
            log.info("表已经存在");
            return;
        }
        Connection connection = getConnection();
        Admin admin=null;
        try {
            admin=connection.getAdmin();
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for(String cf:cfs){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setMaxVersions(versions);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }catch (Exception e){
            e.printStackTrace();
            log.error("e is {}",e.getLocalizedMessage());
            close(connection,admin);
        }
    }

    private static Connection getConnection(){
        Connection connection=null;
        //获取Connection对象
        try {
            connection = ConnectionFactory.createConnection(configuration);
        }catch (Exception e){
            log.error("e is {}",e.getLocalizedMessage());
        }
        return connection;
    }

    private static void close(Connection connection,Admin admin){
        try {
            if(null!=admin){
                admin.close();
            }
            if(null!=connection){
                connection.close();
            }
        }catch (Exception e){
            log.error("close e is {}",e.getLocalizedMessage());
        }
    }
}

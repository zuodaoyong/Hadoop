package com.hbase.constants;

public class HbaseConstants {

    public static final String namespace="weibo";

    public static final String content_table=namespace+":content";

    public static final String content_table_cf="info";

    public static final Integer content_table_version=1;

    public static final String relation_table=namespace+":relation";

    public static final String relation_table_cf1="attends";

    public static final String relation_table_cf2="fans";

    public static final Integer relation_table_version=1;

    public static final String inbox_table=namespace+":inbox";

    public static final String inbox_table_cf="info";

    public static final Integer inbox_table_version=2;
}

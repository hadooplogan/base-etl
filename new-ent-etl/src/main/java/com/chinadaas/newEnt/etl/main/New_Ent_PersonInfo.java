package com.chinadaas.newEnt.etl.main;

import com.chinadaas.newEnt.etl.cfg.Constants;
import com.chinadaas.newEnt.etl.general.GeneralUtils;
import com.chinadaas.newEnt.etl.jdbc.JDBCHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 74061 on 2017/6/13.
 */
public class New_Ent_PersonInfo {
     public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("New_Ent_PersonInfo");
        SparkContext sc = new SparkContext(conf);
        SQLContext hc = new HiveContext(sc);

       /* SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");//设置日期格式
        final java.sql.Date batchDate = GeneralUtils.getColumnsDate(sdf.format(new Date()));*/

        final String currYearDate= GeneralUtils.getYearRangeDate().split("_")[0];
        //在Oracle中新建表
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.createtable("drop table NEW_ENT_PERSONINFO_"+currYearDate);
        jdbcHelper.createtable(createPerTable(currYearDate));

        //1、加载 hive表中数据到dataFrame
        DataFrame df = hc.sql("select " +
                        "pers.S_EXT_NODENUM,pers.PRIPID,pers.PERSON_ID ,pers.NAME ,pers.CERTYPE ,"+
                        "pers.CERNO ,pers.SEX ,pers.NATDATE,pers.DOM ,pers.POSTALCODE ,"+
                        "pers.TEL,pers.LITDEG ,pers.NATION,pers.POLSTAND,pers.OCCST ,"+
                        "pers.OFFSIGN,pers.ACCDSIDE,pers.LEREPSIGN,pers.CHARACTER,pers.COUNTRY ,"+
                        "pers.ARRCHDATE,pers.CERLSSDATE,pers.CERVALPER,pers.CHIOFTHEDELSIGN ,pers.S_EXT_TIMESTAMP,"+
                        "pers.S_EXT_BATCH ,pers.NOTORG ,pers.NOTDOCNO ,pers.S_EXT_SEQUENCE ,pers.S_EXT_VALIDFLAG ,"+
                        "pers.POSITION ,pers.OFFHFROM,pers.OFFHTO,pers.POSBRFORM ,pers.APPOUNIT ,"+
                        "pers.JOBID ,pers.ENTID,pers.TYPEDISPLAY ,pers.DATA_FLAG ,pers.HANDLE_TYPE " +

                " from  NEW_ENT_BASEINFO_MID base left outer join NEW_ENT_PERSONINFO pers  on pers.PRIPID=base.PRIPID " +
                "  and  pers.S_EXT_NODENUM is not null ");

        //2、将 df 转换为 rdd
        JavaRDD<Row> rdd = df.toJavaRDD();

        //3、对rdd进行遍历，将数据保存到Oracle数据库表中 NEW_ENT_INVINFO_20170612
        rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
            private static final long serialVersionUID = -2131762195257207144L;
            //序列化
            @Override
            public void call(Iterator<Row> rows) throws Exception {
                Connection conn = null;
                PreparedStatement st = null;
                try {
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                    conn = DriverManager.getConnection(Constants.JDBC_URL, Constants.JDBC_USER, Constants.JDBC_PASSWORD);
                    conn.setAutoCommit(false);

                    String sql = "insert into NEW_ENT_PERSONINFO_"+currYearDate+" ( "+
                            "S_EXT_NODENUM, PRIPID, PERSON_ID, NAME, CERTYPE,"+
                            "CERNO, SEX, NATDATE, DOM, POSTALCODE,"+
                            "TEL, LITDEG, NATION, POLSTAND, OCCST,"+
                            "OFFSIGN, ACCDSIDE, LEREPSIGN, CHARACTER, COUNTRY,"+
                            "ARRCHDATE, CERLSSDATE, CERVALPER, CHIOFTHEDELSIGN, S_EXT_TIMESTAMP,"+
                            "S_EXT_BATCH, NOTORG, NOTDOCNO, S_EXT_SEQUENCE, S_EXT_VALIDFLAG,"+
                            "POSITION, OFFHFROM, OFFHTO, POSBRFORM, APPOUNIT,"+
                            "JOBID, ENTID, TYPEDISPLAY, DATA_FLAG, HANDLE_TYPE,BATCHDATE) "+
                            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"+
                            "        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"+
                            "        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"+
                            "        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)";

                    st = conn.prepareStatement(sql);
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String s_ext_nodenum = row.getString(0);
                        if(s_ext_nodenum!=null && !"".equals(s_ext_nodenum)){
                            st.setString(1,GeneralUtils.getColumns(row.getString(0)));
                            st.setString(2,GeneralUtils.getColumns(row.getString(1)));
                            st.setString(3,GeneralUtils.getColumns(row.getString(2)));
                            st.setString(4,GeneralUtils.getColumns(row.getString(3)));
                            st.setString(5,GeneralUtils.getColumns(row.getString(4)));
                            st.setString(6,GeneralUtils.getColumns(row.getString(5)));
                            st.setString(7,GeneralUtils.getColumns(row.getString(6)));
                            st.setDate(8,GeneralUtils.getColumnsDate(row.getString(7)));

                            st.setString(9,GeneralUtils.getColumns(row.getString(8)));
                            st.setString(10,GeneralUtils.getColumns(row.getString(9)));
                            st.setString(11,GeneralUtils.getColumns(row.getString(10)));
                            st.setString(12,GeneralUtils.getColumns(row.getString(11)));
                            st.setString(13,GeneralUtils.getColumns(row.getString(12)));
                            st.setString(14,GeneralUtils.getColumns(row.getString(13)));
                            st.setString(15,GeneralUtils.getColumns(row.getString(14)));
                            st.setString(16,GeneralUtils.getColumns(row.getString(15)));
                            st.setString(17,GeneralUtils.getColumns(row.getString(16)));
                            st.setString(18,GeneralUtils.getColumns(row.getString(17)));
                            st.setString(19,GeneralUtils.getColumns(row.getString(18)));
                            st.setString(20,GeneralUtils.getColumns(row.getString(19)));

                            st.setDate(21,GeneralUtils.getColumnsDate(row.getString(20)));
                            st.setDate(22,GeneralUtils.getColumnsDate(row.getString(21)));
                            st.setDate(23,GeneralUtils.getColumnsDate(row.getString(22)));

                            st.setString(24,GeneralUtils.getColumns(row.getString(23)));

                            st.setTimestamp(25, GeneralUtils.getColumnsTimeStamp(row.getString(24)));

                            st.setString(26,GeneralUtils.getColumns(row.getString(25)));
                            st.setString(27,GeneralUtils.getColumns(row.getString(26)));
                            st.setString(28,GeneralUtils.getColumns(row.getString(27)));
                            st.setString(29,GeneralUtils.getColumns(row.getString(28)));
                            st.setString(30,GeneralUtils.getColumns(row.getString(29)));
                            st.setString(31,GeneralUtils.getColumns(row.getString(30)));

                            st.setDate(32,GeneralUtils.getColumnsDate(row.getString(31)));
                            st.setDate(33,GeneralUtils.getColumnsDate(row.getString(32)));

                            st.setString(34,GeneralUtils.getColumns(row.getString(33)));
                            st.setString(35,GeneralUtils.getColumns(row.getString(34)));
                            st.setString(36,GeneralUtils.getColumns(row.getString(35)));
                            st.setString(37,GeneralUtils.getColumns(row.getString(36)));
                            st.setString(38,GeneralUtils.getColumns(row.getString(37)));
                            st.setString(39,GeneralUtils.getColumns(row.getString(38)));

                            st.setInt(40,GeneralUtils.getColumnsInt(row.getString(39)));
                            st.setString(41,currYearDate);
                            //st.setString(41,GeneralUtils.getColumns(row.getString(40)));
                            st.addBatch();
                        }
                    }
                    st.executeBatch();
                    conn.commit();
                    st.clearBatch();
                } catch (Exception e) {
                    if (conn != null) {
                        conn.rollback();
                    }
                    throw(e);
                }finally{
                    if (st != null) {
                        st.close();
                        st = null;
                    }
                    if (conn != null) {
                        conn.close();
                        conn = null;
                    }
                }
            }
        });
        sc.stop();
    }

    private static String createPerTable(String currYearDate) {
         return "create table NEW_ENT_PERSONINFO_"+currYearDate+
                 " ( " +
                 "S_EXT_NODENUM VARCHAR2(40) not null," +
                 "PRIPID VARCHAR2(50)," +
                 "PERSON_ID VARCHAR2(50)," +
                 "NAME VARCHAR2(200)," +
                 "CERTYPE VARCHAR2(64)," +
                 "CERNO VARCHAR2(200)," +
                 "SEX VARCHAR2(64)," +
                 "NATDATE DATE," +
                 "DOM VARCHAR2(500)," +
                 "POSTALCODE VARCHAR2(200)," +
                 "TEL VARCHAR2(200)," +
                 "LITDEG VARCHAR2(64)," +
                 "NATION VARCHAR2(64)," +
                 "POLSTAND VARCHAR2(64)," +
                 "OCCST VARCHAR2(400)," +
                 "OFFSIGN VARCHAR2(64)," +
                 "ACCDSIDE VARCHAR2(500)," +
                 "LEREPSIGN VARCHAR2(64)," +
                 "CHARACTER VARCHAR2(64)," +
                 "COUNTRY VARCHAR2(128)," +
                 "ARRCHDATE DATE," +
                 "CERLSSDATE DATE," +
                 "CERVALPER DATE," +
                 "CHIOFTHEDELSIGN VARCHAR2(1)," +
                 "S_EXT_TIMESTAMP TIMESTAMP(6) not null," +
                 "S_EXT_BATCH VARCHAR2(500)," +
                 "NOTORG VARCHAR2(100)," +
                 "NOTDOCNO VARCHAR2(50)," +
                 "S_EXT_SEQUENCE VARCHAR2(40) not null," +
                 "S_EXT_VALIDFLAG VARCHAR2(1) not null," +
                 "POSITION VARCHAR2(200)," +
                 "OFFHFROM DATE," +
                 "OFFHTO DATE," +
                 "POSBRFORM VARCHAR2(64)," +
                 "APPOUNIT VARCHAR2(500)," +
                 "JOBID VARCHAR2(200)," +
                 "ENTID VARCHAR2(200)," +
                 "TYPEDISPLAY VARCHAR2(200)," +
                 "DATA_FLAG VARCHAR2(1)," +
                 "HANDLE_TYPE NUMBER(2)," +
                 "ZSPID VARCHAR2(50)," +
                 "BATCHDATE VARCHAR2(10)" +
                 " )";
    }
}

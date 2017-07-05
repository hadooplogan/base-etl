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
public class New_Ent_InvInfo {
     public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("New_Ent_InvInfo");
        SparkContext sc = new SparkContext(conf);
        SQLContext hc = new HiveContext(sc);

        //SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");//设置日期格式
        //final java.sql.Date batchDate = GeneralUtils.getColumnsDate(sdf.format(new Date()));

        final String currYearDate= GeneralUtils.getYearRangeDate().split("_")[0];
         //在Oracle中新建表
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.createtable("drop table NEW_ENT_INVINFO_"+currYearDate);
        jdbcHelper.createtable(createInvTable(currYearDate));

        //1、加载 hive表中数据到dataFrame
        DataFrame df = hc.sql("select " +
                "inv.S_EXT_NODENUM,inv.PRIPID,inv.INVID,inv.INV,inv.INVTYPE,"+
                "inv.CERTYPE,inv.CERNO,inv.BLICTYPE,inv.BLICNO,inv.COUNTRY,"+
                "inv.CURRENCY,inv.SUBCONAM,inv.ACCONAM,inv.SUBCONAMUSD,inv.ACCONAMUSD,"+
                "inv.CONPROP,inv.CONFORM,inv.CONDATE,inv.BALDELPER,inv.CONAM,"+
                "inv.EXEAFFSIGN,inv.S_EXT_TIMESTAMP,inv.S_EXT_BATCH,inv.S_EXT_SEQUENCE,inv.S_EXT_VALIDFLAG,"+
                "inv.LINKMAN,inv.JOBID,inv.ENT_ID,inv.TYPEDISPLAY,inv.HANDLE_TYPE" +
                //","+                //"inv.ZSPID"+
                "  From  NEW_ENT_BASEINFO_MID base left outer join NEW_ENT_INVINFO inv  on inv.PRIPID=base.PRIPID " +
                "   and inv.S_EXT_NODENUM is not null");

        //2、将 df 转换为 rdd
        JavaRDD<Row> rdd = df.toJavaRDD();

        //3、对rdd进行遍历，将数据保存到Oracle数据库表中 NEW_ENT_INVINFO20170612
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
                    String sql = "insert into NEW_ENT_INVINFO_"+currYearDate+" (" +
                            "S_EXT_NODENUM,"+
                            "PRIPID,"+
                            "INVID,"+
                            "INV,"+
                            "INVTYPE,"+
                            "CERTYPE,"+
                            "CERNO,"+
                            "BLICTYPE,"+
                            "BLICNO,"+
                            "COUNTRY,"+
                            "CURRENCY,"+
                            "SUBCONAM,"+
                            "ACCONAM,"+
                            "SUBCONAMUSD,"+
                            "ACCONAMUSD,"+
                            "CONPROP,"+
                            "CONFORM,"+
                            "CONDATE,"+
                            "BALDELPER,"+
                            "CONAM,"+
                            "EXEAFFSIGN,"+
                            "S_EXT_TIMESTAMP,"+
                            "S_EXT_BATCH,"+
                            "S_EXT_SEQUENCE,"+
                            "S_EXT_VALIDFLAG,"+
                            "LINKMAN,"+
                            "JOBID,"+
                            "ENT_ID,"+
                            "TYPEDISPLAY,"+
                            "HANDLE_TYPE,BATCHDATE" +
                            // ","+                            //"ZSPID"+
                            ") " +
                            "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";//,?

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
                            st.setString(8,GeneralUtils.getColumns(row.getString(7)));
                            st.setString(9,GeneralUtils.getColumns(row.getString(8)));
                            st.setString(10,GeneralUtils.getColumns(row.getString(9)));
                            st.setString(11,GeneralUtils.getColumns(row.getString(10)));
                            st.setDouble(12,row.getDouble(11));
                            st.setDouble(13,row.getDouble(12));
                            st.setDouble(14,row.getDouble(13));
                            st.setDouble(15,row.getDouble(14));
                            st.setDouble(16,row.getDouble(15));
                            st.setString(17,GeneralUtils.getColumns(row.getString(16)));

                            st.setDate(18, GeneralUtils.getColumnsDate(row.getString(17)));
                            st.setDate(19,GeneralUtils.getColumnsDate(row.getString(18)));
                            st.setDouble(20,row.getDouble(19));
                            st.setString(21,GeneralUtils.getColumns(row.getString(20)));
                            st.setTimestamp(22, GeneralUtils.getColumnsTimeStamp(row.getString(21)));
                            st.setString(23,GeneralUtils.getColumns(row.getString(22)));
                            st.setString(24,GeneralUtils.getColumns(row.getString(23)));
                            st.setString(25,GeneralUtils.getColumns(row.getString(24)));
                            st.setString(26,GeneralUtils.getColumns(row.getString(25)));
                            st.setString(27,GeneralUtils.getColumns(row.getString(26)));
                            st.setString(28,GeneralUtils.getColumns(row.getString(27)));
                            st.setString(29,GeneralUtils.getColumns(row.getString(28)));
                            st.setInt(30,row.getInt(29));
                            st.setString(31,currYearDate);
                            //st.setString(31,row.getString(30));
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

    private static String createInvTable(String currYearDate) {
         return "create table NEW_ENT_INVINFO_"+currYearDate+
                 " (" +
                 "S_EXT_NODENUM VARCHAR2(40) not null," +
                 "PRIPID VARCHAR2(50)," +
                 "INVID VARCHAR2(200)," +
                 "INV VARCHAR2(400)," +
                 "INVTYPE VARCHAR2(64)," +
                 "CERTYPE VARCHAR2(64)," +
                 "CERNO VARCHAR2(200)," +
                 "BLICTYPE VARCHAR2(64)," +
                 "BLICNO VARCHAR2(200)," +
                 "COUNTRY VARCHAR2(128)," +
                 "CURRENCY VARCHAR2(64)," +
                 "SUBCONAM NUMBER(18,6)," +
                 "ACCONAM NUMBER(18,6)," +
                 "SUBCONAMUSD NUMBER(18,6)," +
                 "ACCONAMUSD NUMBER(18,6)," +
                 "CONPROP NUMBER(26,6)," +
                 "CONFORM VARCHAR2(200)," +
                 "CONDATE DATE," +
                 "BALDELPER DATE," +
                 "CONAM NUMBER(18,6)," +
                 "EXEAFFSIGN VARCHAR2(64)," +
                 "S_EXT_TIMESTAMP TIMESTAMP(6) not null," +
                 "S_EXT_BATCH VARCHAR2(500)," +
                 "S_EXT_SEQUENCE VARCHAR2(40) not null," +
                 "S_EXT_VALIDFLAG VARCHAR2(1) not null," +
                 "LINKMAN VARCHAR2(60)," +
                 "JOBID VARCHAR2(200)," +
                 "ENT_ID VARCHAR2(200)," +
                 "TYPEDISPLAY VARCHAR2(200)," +
                 "HANDLE_TYPE NUMBER(2)," +
                 "ZSPID VARCHAR2(50)," +
                 "BATCHDATE VARCHAR2(10)" +
                 " )";
    }

}

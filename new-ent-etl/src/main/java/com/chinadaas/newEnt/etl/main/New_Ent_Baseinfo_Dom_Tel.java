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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 74061 on 2017/6/13.
 */
public class New_Ent_Baseinfo_Dom_Tel {
     public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("New_Ent_Baseinfo_Dom_Tel");
        conf.set("spark.storage.memoryFraction", "0.5");
        conf.set("spark.shuffle.file.buffer", "64");

        //JavaSparkContext sc = new JavaSparkContext(conf);
        SparkContext sc = new SparkContext(conf);
        SQLContext hc = new HiveContext(sc);

        //1、加载 hive表中数据到dataFrame
        DataFrame domDf = hc.sql("select dom,count(1) from NEW_ENT_BASEINFO group by dom having count(1) >2 " );
        DataFrame telDf = hc.sql("select tel,count(1) from NEW_ENT_BASEINFO group by tel having count(1) >2 " );
        //2、将 df 转换为 rdd
        JavaRDD<Row> domRdd = domDf.toJavaRDD();
        JavaRDD<Row> telRdd = telDf.toJavaRDD();

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        //1、删除所有数据
        jdbcHelper.executeUpdate("truncate table NEW_ENT_BASEINFO_DOM_20170619",null);
        jdbcHelper.executeUpdate("truncate table NEW_ENT_BASEINFO_TEL_20170619",null);

        //3、对rdd进行遍历，将数据保存到Oracle数据库表中 NEW_ENT_INVINFO20170612
        domRdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
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

                    String sql = "insert into NEW_ENT_BASEINFO_DOM_20170619 (DOM,COUNT) values(?,?)";

                    st = conn.prepareStatement(sql);
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        st.setString(1, GeneralUtils.getColumns(row.getString(0)));
                        st.setLong(2,row.getLong(1));
                        st.addBatch();
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

        telRdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
             private static final long serialVersionUID = -2131762195257207144L;
             @Override
             public void call(Iterator<Row> rows) throws Exception {
                 Connection conn = null;
                 PreparedStatement st = null;
                 try {
                     Class.forName("oracle.jdbc.driver.OracleDriver");
                     conn = DriverManager.getConnection(Constants.JDBC_URL, Constants.JDBC_USER, Constants.JDBC_PASSWORD);
                     conn.setAutoCommit(false);

                     String sql = "insert into NEW_ENT_BASEINFO_TEL_20170619 (TEL,COUNT) values(?,?)";
                     st = conn.prepareStatement(sql);
                     while (rows.hasNext()) {
                         Row row = rows.next();
                         st.setString(1, GeneralUtils.getColumns(row.getString(0)));
                         st.setLong(2,row.getLong(1));
                         st.addBatch();
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
}

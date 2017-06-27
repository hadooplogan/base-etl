package com.chinadaas.newEnt.etl.main;

import com.chinadaas.newEnt.etl.jdbc.JDBCHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 74061 on 2017/6/13.
 */
public class New_Ent_Baseinfo_Dom_Tel {
     public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("New_Ent_Baseinfo_Dom_Tel");
        conf.set("spark.sql.shuffle.partitions", "475");
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

        //3、对rdd进行遍历，将数据保存到Oracle数据库表中 NEW_ENT_INVINFO20170612
        domRdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
            private static final long serialVersionUID = -2131762195257207144L;
            //序列化
            @Override
            public void call(Iterator<Row> rows) throws Exception {

                JDBCHelper jdbcHelper = JDBCHelper.getInstance();
                String sql = "insert into NEW_ENT_BASEINFO_DOM_20170619 (DOM,COUNT) values(?,?)";
                List<Object[]> params = new ArrayList<>();

                while (rows.hasNext()) {
                    Row row = rows.next();
                    String dom = row.getString(0);
                    Long count = row.getLong(1);
                    params.add(new Object[]{dom,count});
                }
                jdbcHelper.executeBatch(sql, params);
            }
        });

        telRdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
             private static final long serialVersionUID = -2131762195257207144L;
             @Override
             public void call(Iterator<Row> rows) throws Exception {
                 JDBCHelper jdbcHelper = JDBCHelper.getInstance();
                 String sql = "insert into NEW_ENT_BASEINFO_TEL_20170619 (TEL,COUNT) values(?,?)";
                 List<Object[]> params = new ArrayList<>();

                 while (rows.hasNext()) {
                     Row row = rows.next();
                     String tel = row.getString(0);
                     Long count = row.getLong(1);
                     params.add(new Object[]{tel,count});
                 }
                 jdbcHelper.executeBatch(sql, params);
             }
         });
     sc.stop();
    }
}

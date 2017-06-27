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
public class New_Ent_InvInfo {
     public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("New_Ent_InvInfo");
        conf.set("spark.sql.shuffle.partitions", "375");
        conf.set("spark.storage.memoryFraction", "0.5");
        conf.set("spark.shuffle.file.buffer", "64");

        //JavaSparkContext sc = new JavaSparkContext(conf);
        SparkContext sc = new SparkContext(conf);
        SQLContext hc = new HiveContext(sc);

        //1、加载 hive表中数据到dataFrame
        DataFrame df = hc.sql("select " +
                "inv.S_EXT_NODENUM,inv.PRIPID,inv.INVID,inv.INV,inv.INVTYPE,"+
                "inv.CERTYPE,inv.CERNO,inv.BLICTYPE,inv.BLICNO,inv.COUNTRY,"+
                "inv.CURRENCY,inv.SUBCONAM,inv.ACCONAM,inv.SUBCONAMUSD,inv.ACCONAMUSD,"+
                "inv.CONPROP,inv.CONFORM,inv.CONDATE,inv.BALDELPER,inv.CONAM,"+
                "inv.EXEAFFSIGN,inv.S_EXT_TIMESTAMP,inv.S_EXT_BATCH,inv.S_EXT_SEQUENCE,inv.S_EXT_VALIDFLAG,"+
                "inv.LINKMAN,inv.JOBID,inv.ENT_ID,inv.TYPEDISPLAY,inv.HANDLE_TYPE" +
                //","+                //"inv.ZSPID"+
                "  From NEW_ENT_INVINFO inv left outer join NEW_ENT_BASEINFO_MID base on inv.PRIPID=base.PRIPID " +
                " where base.esdate between '20160612' and '20170612' " );


        //2、将 df 转换为 rdd
        JavaRDD<Row> rdd = df.toJavaRDD();

        //3、对rdd进行遍历，将数据保存到Oracle数据库表中 NEW_ENT_INVINFO20170612
        rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
            private static final long serialVersionUID = -2131762195257207144L;
            //序列化
            @Override
            public void call(Iterator<Row> rows) throws Exception {
                JDBCHelper jdbcHelper = JDBCHelper.getInstance();

                String sql = "insert into NEW_ENT_INVINFO_20170619_TEST (" +
                            "S_EXT_NODENUM,PRIPID,INVID,INV,INVTYPE,"+
                            "CERTYPE,CERNO,BLICTYPE,BLICNO,COUNTRY,"+
                            "CURRENCY,SUBCONAM,ACCONAM,SUBCONAMUSD,ACCONAMUSD,"+
                            "CONPROP,CONFORM,CONDATE,BALDELPER,CONAM,"+
                            "EXEAFFSIGN,S_EXT_TIMESTAMP,S_EXT_BATCH,S_EXT_SEQUENCE,S_EXT_VALIDFLAG,"+
                            "LINKMAN,JOBID,ENT_ID,TYPEDISPLAY,HANDLE_TYPE" +
                            // ","+                            //"ZSPID"+
                            ") " +
                            "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";//,?

                List<Object[]> params = new ArrayList<>();

                while (rows.hasNext()) {
                        Row row = rows.next();
                        params.add(new Object[]{
                                GeneralUtils.getColumns(row.getString(0)),
                                GeneralUtils.getColumns(row.getString(1)),
                                GeneralUtils.getColumns(row.getString(2)),
                                GeneralUtils.getColumns(row.getString(3)),
                                GeneralUtils.getColumns(row.getString(4)),
                                GeneralUtils.getColumns(row.getString(5)),
                                GeneralUtils.getColumns(row.getString(6)),
                                GeneralUtils.getColumns(row.getString(7)),
                                GeneralUtils.getColumns(row.getString(8)),
                                GeneralUtils.getColumns(row.getString(9)),
                                GeneralUtils.getColumns(row.getString(10)),
                                row.getDouble(11),
                                row.getDouble(12),
                                row.getDouble(13),
                                row.getDouble(14),
                                row.getDouble(15),
                                GeneralUtils.getColumns(row.getString(16)),
                                GeneralUtils.getColumnsDate(row.getString(17)),
                                GeneralUtils.getColumnsDate(row.getString(18)),
                                row.getDouble(19),
                                GeneralUtils.getColumns(row.getString(20)),
                                GeneralUtils.getColumnsTimeStamp(row.getString(21)),
                                GeneralUtils.getColumns(row.getString(22)),
                                GeneralUtils.getColumns(row.getString(23)),
                                GeneralUtils.getColumns(row.getString(24)),
                                GeneralUtils.getColumns(row.getString(25)),
                                GeneralUtils.getColumns(row.getString(26)),
                                GeneralUtils.getColumns(row.getString(27)),
                                GeneralUtils.getColumns(row.getString(28)),
                                row.getInt(29)
                        });
                }
                jdbcHelper.executeBatch(sql , params);
            }
        });
        sc.stop();
    }

}

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
public class New_Ent_PersonInfo {
     public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("New_Ent_PersonInfo");
        SparkContext sc = new SparkContext(conf);
        SQLContext hc = new HiveContext(sc);

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

                " from NEW_ENT_PERSONINFO pers  left outer join NEW_ENT_BASEINFO base on pers.PRIPID=base.PRIPID " +
                " where base.esdate between '20160612' and '20170612'  " );

        //2、将 df 转换为 rdd
        df.limit(10).show();
        JavaRDD<Row> rdd = df.toJavaRDD();

        //3、对rdd进行遍历，将数据保存到Oracle数据库表中 NEW_ENT_INVINFO_20170612
        rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
            private static final long serialVersionUID = -2131762195257207144L;
            //序列化
            @Override
            public void call(Iterator<Row> rows) throws Exception {
                    JDBCHelper jdbcHelper = JDBCHelper.getInstance();
                    String sql = "insert into NEW_ENT_PERSONINFO_20170619_TEST ( "+
                            "S_EXT_NODENUM, PRIPID, PERSON_ID, NAME, CERTYPE,"+
                            "CERNO, SEX, NATDATE, DOM, POSTALCODE,"+
                            "TEL, LITDEG, NATION, POLSTAND, OCCST,"+
                            "OFFSIGN, ACCDSIDE, LEREPSIGN, CHARACTER, COUNTRY,"+
                            "ARRCHDATE, CERLSSDATE, CERVALPER, CHIOFTHEDELSIGN, S_EXT_TIMESTAMP,"+
                            "S_EXT_BATCH, NOTORG, NOTDOCNO, S_EXT_SEQUENCE, S_EXT_VALIDFLAG,"+
                            "POSITION, OFFHFROM, OFFHTO, POSBRFORM, APPOUNIT,"+
                            "JOBID, ENTID, TYPEDISPLAY, DATA_FLAG, HANDLE_TYPE) "+
                            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"+
                            "        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"+
                            "        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"+
                            "        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
                                GeneralUtils.getColumnsDate(row.getString(7)),
                                GeneralUtils.getColumns(row.getString(8)),
                                GeneralUtils.getColumns(row.getString(9)),
                                GeneralUtils.getColumns(row.getString(10)),
                                GeneralUtils.getColumns(row.getString(11)),
                                GeneralUtils.getColumns(row.getString(12)),
                                GeneralUtils.getColumns(row.getString(13)),
                                GeneralUtils.getColumns(row.getString(14)),
                                GeneralUtils.getColumns(row.getString(15)),
                                GeneralUtils.getColumns(row.getString(16)),
                                GeneralUtils.getColumns(row.getString(17)),
                                GeneralUtils.getColumns(row.getString(18)),
                                GeneralUtils.getColumns(row.getString(19)),
                                GeneralUtils.getColumnsDate(row.getString(20)),
                                GeneralUtils.getColumnsDate(row.getString(21)),
                                GeneralUtils.getColumnsDate(row.getString(22)),
                                GeneralUtils.getColumns(row.getString(23)),
                                GeneralUtils.getColumnsTimeStamp(row.getString(24)),
                                GeneralUtils.getColumns(row.getString(25)),
                                GeneralUtils.getColumns(row.getString(26)),
                                GeneralUtils.getColumns(row.getString(27)),
                                GeneralUtils.getColumns(row.getString(28)),
                                GeneralUtils.getColumns(row.getString(29)),
                                GeneralUtils.getColumns(row.getString(30)),
                                GeneralUtils.getColumnsDate(row.getString(31)),
                                GeneralUtils.getColumnsDate(row.getString(32)),
                                GeneralUtils.getColumns(row.getString(33)),
                                GeneralUtils.getColumns(row.getString(34)),
                                GeneralUtils.getColumns(row.getString(35)),
                                GeneralUtils.getColumns(row.getString(36)),
                                GeneralUtils.getColumns(row.getString(37)),
                                GeneralUtils.getColumns(row.getString(38)),
                                GeneralUtils.getColumnsInt(row.getString(39))
                        });
                    }
                    jdbcHelper.executeBatch(sql , params);
            }
        });
        sc.stop();
    }
}

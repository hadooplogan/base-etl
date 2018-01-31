package com.chinadaas.next.etl.sparksql.main;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

/**
 * 高管表里现阶段重复凉比较大的数据,在此我界定zspid重复大于1000就是重复数据
 * 法人中我拿出大于6000的zspid有大概6个(这几个最影响性能)
 *  打随机前缀与执行
 * 尝试解决数据倾斜的问题
 */

public class  LegalExternalOffice {

    protected static LogUtil logger = LogUtil.getLogger(LegalExternalOffice.class);
    public static void main(String[] args) {
        SparkSession spark = SparkSession.
                builder().
                enableHiveSupport().
                appName("kill_dataskew").
                getOrCreate();

        spark.sqlContext().conf().setConfString("spark.sql.shuffle.partitions", "1600");


        JavaRDD<Row> skewData = getKewDataRdd(spark);
        JavaRDD<Row> javaRDD = getprefix(spark);
        JavaPairRDD<String, String> getJoinRDD = getJoinRDD(spark, skewData, javaRDD);
        getRegisterTmplate(spark, getJoinRDD);
        getnormaldata(spark);
        getUnionData(spark);
        getDistinct(spark);
        Dataset kpi = getKpi(spark);
        DataFrameUtil.saveAsParquetOverwrite(kpi, "/next/skewdatakpi/");

    }

    //拿出倾斜数据集
    private static JavaRDD<Row> getKewDataRdd(SparkSession spark) {

        logger.info("——————拿出倾斜数据———————");
        //拿出人员表中法人的 pripid和zspid
        String hql1 = "select pripid,zspid from e_pri_person_full_20171207 where lerepsign = '1'\n" +
                "and zspid <> '' and zspid <> 'null' and zspid is not null";
        //拿出重复大于6000的数据
        String hql2 = "select zspid,count(0) from lerepsign\n" +
                " group by zspid having count(0) >=6000";
        //拿出倾斜的数据
        String hql3 = "select pripid,zspid from lerepsign where zspid in(select zspid from skewid)";


        spark.sqlContext().sql(hql1).registerTempTable("lerepsign");
        spark.sqlContext().sql(hql2).registerTempTable("skewid");
        Dataset<Row> dataset = spark.sqlContext().sql(hql3);
        JavaRDD<Row> rowJavaRDD = dataset.toJavaRDD();
        logger.info("——————成功拿出倾斜数据————————");

        return rowJavaRDD;

    }

    //拿出倾斜数据 中唯一的企业对应法人数据
    public static JavaRDD<Row> getprefix(SparkSession spark) {

        logger.info("————————拿出唯一的企业对应法人数据————————");
        String hql4 = "select pripid,zspid from enterprisebaseinfocollect_full_20171207 where zspid in (select zspid from skewid)";
        String hql5 = "select * from mainskew where zspid <> '' and zspid <> 'null' and zspid is not null";
        spark.sqlContext().sql(hql4).registerTempTable("mainskew");
        Dataset<Row> dataset = spark.sqlContext().sql(hql5);
        JavaRDD<Row> rdd = dataset.toJavaRDD();
        logger.info("————————完成拿出唯一的企业对应法人数据————————");
        return rdd;
    }

    //对倾斜数据集进行扩容 和 随机数打标签

    private static JavaPairRDD<String, String> getJoinRDD(SparkSession spark, JavaRDD<Row> rdd, JavaRDD<Row> rdd1) {

        //打上随机的前缀（对我们拿到的企业对应法人的数据 对zspid打上标签）
        JavaPairRDD<String, String> mappedrdd = rdd1.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                Random random = new Random();
                int prefix = random.nextInt(100);
                return new Tuple2<String, String>(prefix + "_" + row.getString(1), row.getString(0));
            }
        });


        //扩容100倍(对 person表的重复数据集)
        JavaPairRDD<String, String> pairRDD = rdd.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row row) throws Exception {
                ArrayList<Row> rows = new ArrayList<>();
                for (int i = 0; i < 100; i++) {

                    Row _row = RowFactory.create(i + "_" + row.get(1), row.get(0));

                    rows.add(_row);
                }
                return rows.iterator();
            }
        }).mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getString(0), row.getString(1));
            }
        });

        //把join后的数据，进行转化,便于转化成dataframe
        JavaPairRDD<String, String> pair = mappedrdd.join(pairRDD, 800).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> s) throws Exception {
                return new Tuple2<String, String>(s._2._1, s._2._2);
            }
        });

        return pair;
    }

    // 把倾斜数据关联后的结果生成临时表
    private static void getRegisterTmplate(SparkSession spark, JavaPairRDD<String, String> pairRDD) {

        JavaRDD<Row> rdd = pairRDD.map(new Function<Tuple2<String, String>, Row>() {
            @Override
            public Row call(Tuple2<String, String> v1) throws Exception {
                Row row = RowFactory.create(v1._1, v1._2);

                return row;
            }
        });

        //加上schema信息
        StructType _schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("pripid", DataTypes.StringType, true),
                DataTypes.createStructField("x", DataTypes.StringType, true)));

        Dataset<Row> dataFrame = spark.sqlContext().createDataFrame(rdd, _schema);

        dataFrame.registerTempTable("tmpskew");
    }

    //拿到均匀数据集的结果
    private static void getnormaldata(SparkSession spark) {
        logger.info("————————拿出均匀数据集————————");
        String hql1 = "select pripid,zspid from e_pri_person_full_20171207 where zspid not in(select zspid from skewid)";
        String hql2 = "select pripid,zspid from enterprisebaseinfocollect_full_20171207 where zspid not in(select zspid from skewid)";
        String hql3 = "select a.pripid,b.pripid as x from nomarljoin a join normaldata b on a.zspid = b.zspid\n"+
                      "and a.zspid is not null and a.zspid <> '' and a.zspid <> 'null'\n"+
                      "and b.zspid is not null and b.zspid <> '' and b.zspid <> 'null'";

        spark.sqlContext().sql(hql1).registerTempTable("normaldata");
        spark.sqlContext().sql(hql2).registerTempTable("nomarljoin");
        spark.sqlContext().sql(hql3).registerTempTable("normal");
        logger.info("————————成功拿出均匀数据集————————");

    }

    //union正常数据集和倾斜数据集
    private static Dataset getUnionData(SparkSession spark) {

        String hql = "select * from normal union all select * from tmpskew";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpunion");
    }

    //按照逻辑去重数据，对存在的一家企业多任职进行剔除
    private static Dataset getDistinct(SparkSession spark) {

        String hql = "select distinct a.pripid,c.pripid as y,c.entstatus from \n" +
                "enterprisebaseinfocollect_full_20171207 a join\n"+
                "tmpunion b\n" +
                "on a.pripid = b.pripid\n"+
                "join enterprisebaseinfocollect_full_20171207 c on b.x = c.pripid \n" +
                "where a.entname <> c.entname";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdistinct");
    }

    //得到统计结果
    private static Dataset getKpi(SparkSession spark) {

        String hql = "select pripid,\n" +
                "count(y) as eb0041," +
                "sum(case when entstatus = '1' then 1 else 0 end) as eb0042\n" +
                "from tmpdistinct\n" +
                "group by pripid";

        return DataFrameUtil.getDataFrame(spark, hql, "kpi");
    }

}

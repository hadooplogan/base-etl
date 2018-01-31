package com.chinadaas.association.etl.demo;

import com.chinadaas.association.etl.sparksql.Hdfs2EsETL;
import com.chinadaas.association.etl.table.EntBaseInfoEO;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.IDataAdapter;
import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.udf.StringFormatUDF;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by gongxs01 on 2017/7/17.
 */
public class Es2HdfsDemoApp {

    static IDataAdapter ida = new EsDataAdapterImpl();

    public static void main(String[] args) {
/*
        SparkSession spark = SparkSession
                .builder()
                .appName("Chinadaas Hdfs2Es ETL APP")
                .enableHiveSupport()
                .getOrCreate();

        Hdfs2EsETL hdfs = new Hdfs2EsETL();

        Map cfg = new HashMap<String,String>();

        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        cfg.put(DatabaseValues.ES_NODES,"192.168.206.12");
        cfg.put(DatabaseValues.ES_PORT,"9300");
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        cfg.put("es.mapping.id","id");
        cfg.put("es.resource.write","xxxx/xxxxx");

        ida.WriteData(hdfs.getAlterDataDF(spark),cfg);


        spark.stop();*/


        //model=inc 增量方式 model=all 全量模式

        SparkConf conf = new SparkConf();
        conf.setAppName("EntInfo2Es4ShangHaiTelecomApp");
        conf.set(DatabaseValues.ES_INDEX_AUTO_CREATE, "false");
        conf.set(DatabaseValues.ES_NODES,"192.168.207.11,192.168.207.12,192.168.207.13");
        conf.set(DatabaseValues.ES_PORT,"59200");
        conf.set(DatabaseValues.ES_BATCH_SIZE_BYTES,"4m");
        conf.set(DatabaseValues.ES_BATCH_SIZE_ENTRIES,"50000");


        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        Map cfg = new HashMap<String,String>();

        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        cfg.put("es.mapping.id","pripid");
        cfg.put("es.resource.read","position/position");


     /*   Map cfg2 = new HashMap<String,String>();

        cfg2.put(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg2.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        cfg2.put("es.mapping.id","pripid");
        cfg2.put("es.resource.read","position_1207/position");*/

        Dataset dataset = (Dataset)ida.loadData(spark, cfg);
//        Dataset dataset2 = (Dataset)ida.loadData(spark, cfg2);


        dataset.registerTempTable("position");
//        dataset2.registerTempTable("position2017");

        RegisterTable.regiserEntBaseInfoTable(spark,"enterprisebaseinfocollect","20171207","/tmp/chinadaas_20171207/ENTERPRISEBASEINFOCOLLECT");
//        spark.sql("select a.* from position a  where not exists (select 1 from position2017 b  where a.pripid=b.pripid)").limit("555").s
//        spark.sql("select a.*,b.entname from position a join enterprisebaseinfocollect b on a.pripid=b.pripid").limit(10).show();

        EsSparkSQL.saveToEs(spark.sql("select a.*,b.entname from position a join enterprisebaseinfocollect b on a.pripid=b.pripid"),
                "position_1213/position", JavaConversions.mapAsScalaMap(ImmutableMap.of("es.mapping.id", "pripid")));

        spark.close();
    }
}

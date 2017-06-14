package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.sparksql.FlushRadioETL;
import com.chinadaas.common.udf.CollectionSameUDF;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by gongxs01 on 2017/6/13.
 * 刷股东占比数据
 */
public class App3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chinadaas Association ETL APP3");
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        FlushRadioETL dfEtl = new FlushRadioETL();
        CollectionSameUDF.collectSame(sc,sqlContext);
        dfEtl.getFlushBadData(sqlContext).write().mode(SaveMode.Overwrite).parquet("/tmp/hive_export_inv/e_inv_investment_parquet");
        sqlContext.clearCache();
        sc.stop();
    }
}

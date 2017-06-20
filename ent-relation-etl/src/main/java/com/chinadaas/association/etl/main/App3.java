package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.sparksql.FlushConpropETL;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
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
        FlushConpropETL dfEtl = new FlushConpropETL();
        CollectionSameUDF.collectSame(sc,sqlContext);
        dfEtl.getFlushBadData(sqlContext).write().mode(SaveMode.Overwrite).parquet(CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_INV_RADIO_PATH));
        sqlContext.clearCache();
        sc.stop();
    }
}

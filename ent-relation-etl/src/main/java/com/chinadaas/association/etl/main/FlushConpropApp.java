package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.sparksql.FlushConpropETL;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.udf.CollectionSameUDF;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by gongxs01 on 2017/6/13.
 *
 * ***************************************
 *
 * 采用年报数据刷新股东占比信息
 * 并重新计算占比信息=认缴出资额/企业注册资本
 *
 * ****************************************
 *
 */
public class FlushConpropApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chinadaas Association ETL APP3");


        SparkSession spark = SparkSession
                .builder()
                .appName("Chinadaas Association ETL APP3")
                .enableHiveSupport()
                .getOrCreate();

        FlushConpropETL dfEtl = new FlushConpropETL();
        dfEtl.setDate(args[0]);
        CollectionSameUDF.collectSame(spark);
        DataFrameUtil.saveAsParquetOverwrite(dfEtl.getFlushBadData(spark),CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP)+
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_INV_PARQUET_PATH));
        spark.stop();
    }

    public static void flushConprop(SparkSession sqlContext,String date){
        FlushConpropETL dfEtl = new FlushConpropETL();
        CollectionSameUDF.collectSame(sqlContext);
        dfEtl.setDate(date);
        DataFrameUtil.saveAsParquetOverwrite(dfEtl.getFlushBadData(sqlContext),CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP)+
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_INV_PARQUET_PATH));
    }

}

package com.chinadaas.association.etl.common;

import com.chinadaas.association.etl.main.FlushConpropApp;
import com.chinadaas.association.etl.sparksql.CommonETL;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;

/**
 * Created by gongxs01 on 2017/7/25.
 */
public class CommonApp implements Serializable {

    private static final String parquetPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP);

    public static String ENT_INFO = "entInfoTmp03";
    public static String ENT_INV_INFO = "entInvTmp";
    public static String ENT_PERSON_INFO = "entPersonTmp";

    public static void main(String[] args) {

        String date = args[0];

        SparkConf conf = new SparkConf().setAppName("Chinadaas Common ETL APP");
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);

        saveDataFrameAsParquet(sqlContext,date);
        FlushConpropApp.flushConprop(sqlContext,date);

        sqlContext.clearCache();
        sc.stop();
    }

    private static void saveDataFrameAsParquet(HiveContext sqlContext,String date){
        DataFrameUtil.saveAsParquetOverwrite(CommonETL.getEntInfo(sqlContext,date), parquetPath + ENT_INFO);
        DataFrameUtil.saveAsParquetOverwrite(CommonETL.getEntInvInfo(sqlContext,date), parquetPath + ENT_INV_INFO);
        DataFrameUtil.saveAsParquetOverwrite(CommonETL.getEntPersonInfo(sqlContext,date),parquetPath+ENT_PERSON_INFO);
    }


    public static void loadAndRegiserTable(HiveContext sqlContext,String[] tableNames){
        for(int i=0;i<tableNames.length;i++){
            sqlContext.load(parquetPath + tableNames[i]).registerTempTable(tableNames[i]);
        }
    }



}

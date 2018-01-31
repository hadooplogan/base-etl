package com.chinadaas.preprocess.etl.main;

import com.chinadaas.preprocess.etl.conver.IConvert;
import com.chinadaas.preprocess.etl.conver.impl.LegalStaffCovertImpl;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/11/29.
 *
 *  法人对外任职
 *
 */
public class LegalStaffApp {

    final static IConvert convert = new LegalStaffCovertImpl();


    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("Chinadaas Preprocess ETL APP")
                .enableHiveSupport()
                .getOrCreate();

        convert.convertData(spark,args);

        spark.stop();
    }
}

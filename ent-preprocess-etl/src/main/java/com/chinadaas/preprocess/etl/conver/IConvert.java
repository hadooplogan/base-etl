package com.chinadaas.preprocess.etl.conver;


import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/9/12.
 */
public interface IConvert {

    void convertData(SparkSession spark, String[] args);

}

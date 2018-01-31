package com.chinadaas.next.etl.sparksql.main;

import com.chinadaas.next.etl.sparksql.convert.IConvert;
import com.chinadaas.next.etl.sparksql.convert.impl.EntRelationCovertImpl;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/9/12.
 *
 * 关联洞察相关指标
 *
 */
public class EntRelationBiKpiApp {

    final static IConvert convert = new EntRelationCovertImpl();


    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Chinadaas NEXT-RELATION ETL APP")
                .enableHiveSupport()
                .getOrCreate();

        convert.convertData(spark,args[0]);

        spark.stop();

    }
}

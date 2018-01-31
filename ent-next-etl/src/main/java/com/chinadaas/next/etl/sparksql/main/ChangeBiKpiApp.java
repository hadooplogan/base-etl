package com.chinadaas.next.etl.sparksql.main;

import com.chinadaas.next.etl.sparksql.convert.IConvert;
import com.chinadaas.next.etl.sparksql.convert.impl.ChangeCovertImpl;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/9/21.
 */
public class ChangeBiKpiApp {
    final static IConvert convert = new ChangeCovertImpl();


    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Chinadaas NEXT-CHANGE ETL APP")
                .enableHiveSupport()
                .getOrCreate();

        convert.convertData(spark,args[0]);

        spark.stop();



    }

}

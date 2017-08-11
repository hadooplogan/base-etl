package com.chinadaas.association.etl.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gongxs01 on 2017/6/12.
 *
 *
 * ****************************
 *
 *  oracle to hdfs  demo
 *
 * *****************************
 *
 */
public class Oracle2HdfsDemoApp {

    public static void main(String[] args) {

        /*SparkConf conf = new SparkConf().setAppName("Chinadaas Oracle To HDFS");
        String tableName = args[0];
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:oracle:thin:DM_RISKBELL/DM_RISKBELL@192.168.205.30:1521/orcl");
        options.put("driver", "oracle.jdbc.OracleDriver");
        options.put("dbtable", tableName);
        options.put("numPartitions", "4");
        options.put("fetchsize", "1000");
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        DataFrame jdbcDF = sqlContext.read().format("jdbc").options(options).load();

        jdbcDF.write().mode(SaveMode.Overwrite).parquet("/tmp/hive_export_inv/tmp/"+tableName);
        sqlContext.clearCache();
        sc.stop();*/

    }
}

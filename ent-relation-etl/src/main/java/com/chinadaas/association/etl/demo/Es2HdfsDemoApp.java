package com.chinadaas.association.etl.demo;

import com.chinadaas.association.etl.sparksql.Hdfs2EsETL;
import com.chinadaas.association.etl.table.EntBaseInfoEO;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.udf.StringFormatUDF;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.elasticsearch.spark.sql.EsSparkSQL;



/**
 * Created by gongxs01 on 2017/7/17.
 */
public class Es2HdfsDemoApp {
    public static void main(String[] args) {
      /*  SparkConf conf = new SparkConf().setAppName("Chinadaas Es2Hdfs ETL APP");
        conf.set(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        conf.set(DatabaseValues.ES_NODES,"192.168.206.12");
        conf.set(DatabaseValues.ES_PORT,"9300");
        conf.set(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        conf.set(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        EsSparkSQL.esDF(sqlContext,"code/code").saveAsParquetFile("/tmp/hive_export_inv/code_parquet");
        sqlContext.clearCache();
        sc.stop();*/
    }
}

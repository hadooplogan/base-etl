package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.sparksql.Hdfs2EsETL;
import com.chinadaas.association.etl.table.EntBaseInfoEO;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.udf.StringFormatUDF;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

/**
 * Created by gongxs01 on 2017/5/15.
 */
public class App1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chinadaas Hbase2Es ETL APP");
        conf.set(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        conf.set(DatabaseValues.ES_NODES,CommonConfig.getValue(DatabaseValues.ES_NODES));
        conf.set(DatabaseValues.ES_PORT,CommonConfig.getValue(DatabaseValues.ES_PORT));
        conf.set(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        conf.set(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        StringFormatUDF.stringHandle(sc,sqlContext);
        JavaEsSpark.saveToEs(EntBaseInfoEO.convertData(sqlContext),"entbaseinfo_test/ENTBASEINFO_TEST");
        sqlContext.clearCache();
        sc.stop();

    }

}

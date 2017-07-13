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
import org.elasticsearch.spark.sql.EsSparkSQL;

/**
 * Created by gongxs01 on 2017/5/15.
 * ****************************************
 *企业照面信息，股东信息，管理人员信息写入ES entbaseinfo
 *企业变更数据写入ES ealterrecoder
 * ****************************************
 */
public class EntInfo2EsApp {
    public static void main(String[] args) {

        String date = args[0];

        SparkConf conf = new SparkConf().setAppName("Chinadaas Hdfs2Es ETL APP");
        conf.set(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        conf.set(DatabaseValues.ES_NODES,CommonConfig.getValue(DatabaseValues.ES_NODES));
        conf.set(DatabaseValues.ES_PORT,CommonConfig.getValue(DatabaseValues.ES_PORT));
        conf.set(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        conf.set(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        StringFormatUDF.stringHandle(sc,sqlContext);
        Hdfs2EsETL hdfs = new Hdfs2EsETL();
        hdfs.setDate(date);
        String parquetPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP);
        //共同企业基本信息
        sqlContext.load(parquetPath+"entInfoTmp03").registerTempTable("entInfoTmp03");
        //企业投资
        JavaEsSpark.saveToEs(EntBaseInfoEO.convertData(sqlContext,hdfs),"entbaseinfo_test/ENTBASEINFO_TEST");
        EsSparkSQL.saveToEs(hdfs.getAlterDataDF(sqlContext),"ealterrecoder/EALTERRECODER");
        sqlContext.clearCache();
        sc.stop();
    }

}

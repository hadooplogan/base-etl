package com.chinadaas.association.etl.main;

import com.chinadaas.association.common.CommonConfig;
import com.chinadaas.association.common.DatabaseValues;
import com.chinadaas.association.etl.sparksql.DirectedAssociationETL;
import com.chinadaas.association.etl.sparksql.Hbase2EsETL;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
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
        conf.set("es.http.timeout ","30m");
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);

        Hbase2EsETL dfEtl = new Hbase2EsETL();
        //person
        //JavaEsSparkSQL.saveToEs(dfEtl.getPersonDataFrame(sqlContext).repartition(40),"person/Person");
        JavaEsSparkSQL.saveToEs(dfEtl.getEntDataFrame(sqlContext),"entbaseinfo_test01/ENTBASEINFO_TEST01");
       // JavaEsSparkSQL.saveToEs(dfEtl.getinvDataFrame(sqlContext).repartition(40),"inv/INV");
        sqlContext.clearCache();
        sc.stop();

    }
}
